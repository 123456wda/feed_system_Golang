package video

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"gorm.io/gorm"
)

type CommentService struct {
	repo         *CommentRepository
	videoRepo    *VideoRepository
	cache        *rediscache.Client
	commentMQ    *rabbitmq.CommentMQ
	popularityMQ *rabbitmq.PopularityMQ
}

func NewCommentService(repo *CommentRepository, videoRepo *VideoRepository, cache *rediscache.Client, commentMQ *rabbitmq.CommentMQ, popularityMQ *rabbitmq.PopularityMQ) *CommentService {
	return &CommentService{repo, videoRepo, cache, commentMQ, popularityMQ}
}

// Publish 发布评论。
// 流程：参数校验 → 确认视频存在 → MQ 异步投递（优先）→ 降级同步事务写入（兜底）。
func (s *CommentService) Publish(ctx context.Context, comment *Comment) error {
	if comment == nil {
		return errors.New("comment is nil")
	}
	comment.Username = strings.TrimSpace(comment.Username)
	comment.Content = strings.TrimSpace(comment.Content)
	if comment.VideoID == 0 || comment.AuthorID == 0 {
		return errors.New("video_id and author_id are required")
	}
	if comment.Content == "" {
		return errors.New("content is required")
	}

	// 确认目标视频存在
	exists, err := s.videoRepo.IsExist(ctx, comment.VideoID)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("video not found")
	}

	// === MQ 优先路径：异步投递，不阻塞请求 ===
	commentMQSent := false
	popularityMQSent := false

	if s.commentMQ != nil {
		if err := s.commentMQ.Publish(ctx, comment.Username, comment.VideoID, comment.AuthorID, comment.Content); err == nil {
			commentMQSent = true
		}
	}
	if s.popularityMQ != nil {
		if err := s.popularityMQ.Update(ctx, comment.VideoID, 1); err == nil {
			popularityMQSent = true
		}
	}

	// 两条 MQ 都投递成功 → 直接返回，消费者会异步处理
	if commentMQSent && popularityMQSent {
		s.invalidateCommentCache(ctx, comment.VideoID)
		return nil
	}

	// === Fallback：同步 MySQL 事务 ===
	// CommentMQ 投递失败 → 需要同步写入（评论记录 + 视频热度必须在同一事务里）
	if !commentMQSent {
		err := s.repo.Transaction(ctx, func(tx *gorm.DB) error {
			// 校验视频存在（事务内，防并发删除）
			if err := tx.Select("id").First(&Video{}, comment.VideoID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return errors.New("video not found")
				}
				return err
			}
			// 写入评论
			if err := s.repo.CreateCommentInTx(ctx, tx, comment); err != nil {
				return err
			}
			// 原子增加视频热度
			return s.videoRepo.ChangePopularity(ctx, tx, comment.VideoID, 1)
		})
		if err != nil {
			return err
		}
	}

	// PopularityMQ 投递失败 → 同步更新 Redis 热度缓存
	if !popularityMQSent {
		UpdatePopularityCache(ctx, s.cache, comment.VideoID, 1)
	}

	s.invalidateCommentCache(ctx, comment.VideoID)
	return nil
}

// Delete 删除评论。
// 流程：查评论 → 校验所有权 → MQ 异步投递（优先）→ 降级同步删除（兜底）。
func (s *CommentService) Delete(ctx context.Context, commentID uint, accountID uint) error {
	// 查询评论是否存在
	comment, err := s.repo.GetByID(ctx, commentID)
	if err != nil {
		return err
	}
	if comment == nil {
		return errors.New("comment not found")
	}
	// 权限校验：只有评论作者本人才能删除
	if comment.AuthorID != accountID {
		return errors.New("permission denied")
	}

	// MQ 优先：投递删除事件，消费者异步处理
	if s.commentMQ != nil {
		if err := s.commentMQ.Delete(ctx, commentID); err == nil {
			s.invalidateCommentCache(ctx, comment.VideoID)
			return nil
		}
	}

	// Fallback：同步删除
	if err := s.repo.DeleteComment(ctx, comment); err != nil {
		return err
	}
	s.invalidateCommentCache(ctx, comment.VideoID)
	return nil
}

// GetAll 查询某视频下的评论（分页 + Redis 缓存）。
// 缓存 key: comment:list:video:{videoID}:page:{page}:size:{pageSize}
// TTL 30s，评论发布/删除时主动失效。
func (s *CommentService) GetAll(ctx context.Context, videoID uint, page, pageSize int) ([]Comment, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}
	offset := (page - 1) * pageSize

	// 尝试从 Redis 缓存读取
	cacheKey := fmt.Sprintf("comment:list:video:%d:page:%d:size:%d", videoID, page, pageSize)
	if s.cache != nil {
		if data, err := s.cache.GetBytes(ctx, cacheKey); err == nil && len(data) > 0 {
			var comments []Comment
			if json.Unmarshal(data, &comments) == nil {
				return comments, nil
			}
		}
	}

	// 缓存未命中，查 MySQL（不再做 IsExist 检查，空列表语义等价）
	comments, err := s.repo.GetCommentsByPage(ctx, videoID, offset, pageSize)
	if err != nil {
		return nil, err
	}

	// 回写缓存，TTL 30s
	if s.cache != nil {
		if data, err := json.Marshal(comments); err == nil {
			_ = s.cache.SetBytes(ctx, cacheKey, data, 30*time.Second)
		}
	}

	return comments, nil
}

// invalidateCommentCache 失效评论缓存（发布/删除评论时调用）。
func (s *CommentService) invalidateCommentCache(ctx context.Context, videoID uint) {
	if s.cache == nil {
		return
	}
	// 只失效第 1 页（最常见的查询），生产环境可用 pattern scan 或版本号方案
	s.cache.Del(ctx, fmt.Sprintf("comment:list:video:%d:page:1:size:20", videoID))
}
