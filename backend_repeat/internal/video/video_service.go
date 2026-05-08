package video

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"feedsystem_video_go/internal/middleware/rabbitmq"

	rediscache "feedsystem_video_go/internal/middleware/redis"
)

var ErrInvalidAuthorID = errors.New("invalid author_id")

type VideoService struct {
	repo     *VideoRepository
	rmq      *rabbitmq.PopularityMQ
	cache    *rediscache.Client
	cacheTTL time.Duration
}

func NewVideoService(repo *VideoRepository, cache *rediscache.Client, rmq *rabbitmq.PopularityMQ) *VideoService {
	if repo == nil || cache == nil || rmq == nil {
		panic("nil pointer")
	}
	return &VideoService{repo, rmq, cache, 5 * time.Minute}
}

func (s *VideoService) ListByAuthorID(ctx context.Context, authorID uint) ([]Video, error) {
	if authorID <= 0 {
		return nil, ErrInvalidAuthorID
	}

	return s.repo.ListByAuthorID(ctx, authorID)
}

func (s *VideoService) GetDetail(ctx context.Context, id uint) (*Video, error) {
	cacheKey := fmt.Sprintf("video:detail:id=%d", id)
	// 封装两个对缓存操作的闭包函数
	GetCache := func() (*Video, error) {
		cacheCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()

		b, err := s.cache.GetBytes(cacheCtx, cacheKey)
		if err != nil {
			return nil, err
		}
		var cached Video
		err = json.Unmarshal(b, &cached)
		if err != nil {
			return nil, err
		}
		return &cached, nil
	}

	SetCached := func(video *Video) error {
		b, err := json.Marshal(video)
		if err != nil {
			return err
		}
		cacheCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		return s.cache.SetBytes(cacheCtx, cacheKey, b, s.cacheTTL)
	}

	// 先在缓存读取
	if s.cache != nil {
		video, err := GetCache()
		if err == nil {
			return video, nil
		} else if rediscache.IsMiss(err) {
			lockKey := "lock:" + cacheKey
			lockCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			token, ok, err := s.cache.Lock(lockCtx, lockKey, 2*time.Second)
			if err != nil {
				log.Printf("warning redis cache(getDetail): %v", err)
			}
			if err == nil && ok {
				defer func() {
					if err := s.cache.UnLock(context.Background(), lockKey, token); err != nil {
						log.Printf("warning redis cache(getDetail): %v", err)
					}
				}()
				// 开始去数据库里面查找

				v, err := GetCache()
				if err == nil {
					return v, nil
				}

				video, err := s.repo.GetDetail(ctx, id)
				if err != nil {
					return nil, err
				}
				if err := SetCached(video); err != nil {
					log.Printf("warning redis cache(getDetail): %v", err)
				}
				return video, nil
			}

			// 没拿到锁：等待别人回填缓存
			for i := 0; i < 5; i++ {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(20 * time.Millisecond):
				}
				if v, err := GetCache(); err == nil {
					return v, nil
				}
			}

		} else {
			log.Printf("warning redis cache(getDetail): %v", err)
		}
	}

	// mysql查找后回源redis
	video, err := s.repo.GetDetail(ctx, id)
	if err != nil {
		return nil, err
	}
	if s.cache != nil {
		if err := SetCached(video); err != nil {
			log.Printf("warning redis cache(getDetail): %v", err)
		}
	}
	return video, nil
}

func (s *VideoService) PublishVideo(ctx context.Context, video *Video) error {
	if video == nil {
		return errors.New("video is nil")
	}
	video.Title = strings.TrimSpace(video.Title)
	video.PlayURL = strings.TrimSpace(video.PlayURL)
	video.CoverURL = strings.TrimSpace(video.CoverURL)

	if video.Title == "" {
		return errors.New("title is required")
	}
	if video.PlayURL == "" {
		return errors.New("play url is required")
	}
	if video.CoverURL == "" {
		return errors.New("cover url is required")
	}
	// 进行初步参数校验
	return s.repo.PublishVideo(ctx, video)
}

// Delete 删除视频，先校验视频存在性和所有权，再删除数据库记录，最后清除 Redis 缓存。
func (s *VideoService) Delete(ctx context.Context, id uint, authorID uint) error {
	// 第一步：根据 ID 查询视频是否存在
	video, err := s.repo.GetDetail(ctx, id)
	if err != nil {
		return err
	}
	// 视频不存在则直接返回错误
	if video == nil {
		return errors.New("video not found")
	}
	// 权限校验：只有视频作者本人才能删除
	if video.AuthorID != authorID {
		return errors.New("unauthorized")
	}
	// 执行数据库删除
	if err := s.repo.DeleteVideo(ctx, id); err != nil {
		return err
	}
	// 删除成功后清除详情缓存，避免用户读到已删除视频的旧数据
	if s.cache != nil {
		cacheKey := fmt.Sprintf("video:detail:id=%d", id)
		_ = s.cache.Del(context.Background(), cacheKey)
	}
	return nil
}

// UpdateLikesCount 设置视频的点赞数为指定绝对值（供 like worker 同步使用）。
func (s *VideoService) UpdateLikesCount(ctx context.Context, id uint, likesCount int64) error {
	return s.repo.SetLikesCount(ctx, id, likesCount)
}
