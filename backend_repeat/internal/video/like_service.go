package video

import (
	"context"
	"errors"
	"time"

	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"gorm.io/gorm"
)

// LikeService 处理点赞相关业务逻辑。
// 设计原则：MQ 优先异步写入，MQ 不可用时降级为同步 MySQL 事务。
// 优化点（相比原始版本）：
//   1. 移除前置 IsLiked 检查，依赖唯一索引防重，消除 TOCTOU 竞态窗口
//   2. Service 不直接访问 repo.db，事务操作通过 LikeRepository.Transaction + 事务方法
//   3. 变量命名纠正：likeMQSent / popularityMQSent 明确表达 MQ 投递状态
type LikeService struct {
	likeRepo     *LikeRepository
	videoRepo    *VideoRepository
	cache        *rediscache.Client
	likeMQ       *rabbitmq.LikeMQ
	popularityMQ *rabbitmq.PopularityMQ
}

func NewLikeService(
	likeRepo *LikeRepository,
	videoRepo *VideoRepository,
	cache *rediscache.Client,
	likeMQ *rabbitmq.LikeMQ,
	popularityMQ *rabbitmq.PopularityMQ,
) *LikeService {
	return &LikeService{
		likeRepo:     likeRepo,
		videoRepo:    videoRepo,
		cache:        cache,
		likeMQ:       likeMQ,
		popularityMQ: popularityMQ,
	}
}

// Like 点赞操作。
// 流程：
//   1. 参数校验
//   2. 确认目标视频存在
//   3. 尝试 MQ 异步投递（优先路径）
//   4. MQ 失败 → 降级为同步 MySQL 事务 + Redis 热度更新
func (s *LikeService) Like(ctx context.Context, accountID, videoID uint) error {
	// 参数校验：用户ID和视频ID缺一不可
	if accountID == 0 || videoID == 0 {
		return errors.New("video_id and account_id are required")
	}

	// 确认视频存在——这里只校验一次，事务内不再重复校验
	if s.videoRepo != nil {
		ok, err := s.videoRepo.IsExist(ctx, videoID)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("video not found")
		}
	}

	like := &Like{
		VideoID:   videoID,
		AccountID: accountID,
		CreatedAt: time.Now(),
	}

	// === MQ 优先路径：异步投递，不阻塞请求 ===
	// 注意：MQ 路径不检查是否已点赞——唯一索引校验由消费者负责，
	// 消费者遇 1062 重复键时静默跳过（幂等）。
	likeMQSent := false
	popularityMQSent := false

	if s.likeMQ != nil {
		if err := s.likeMQ.Like(ctx, accountID, videoID); err == nil {
			likeMQSent = true
		}
	}
	if s.popularityMQ != nil {
		if err := s.popularityMQ.Update(ctx, videoID, 1); err == nil {
			popularityMQSent = true
		}
	}

	// 两条 MQ 都投递成功 → 直接返回，消费者会异步写入 MySQL + Redis
	if likeMQSent && popularityMQSent {
		return nil
	}

	// === Fallback：同步 MySQL 事务 ===
	// LikeMQ 投递失败 → 需要同步写入 MySQL（点赞记录 + 计数更新）
	if !likeMQSent {
		err := s.likeRepo.Transaction(ctx, func(tx *gorm.DB) error {
			// 插入点赞记录，遇重复键报错（不用 IsLiked 预检，消除 TOCTOU）
			if err := s.likeRepo.LikeInTx(ctx, tx, like); err != nil {
				return err
			}

			// 原子增加点赞数
			if err := s.videoRepo.ChangeLikesCount(ctx, tx, videoID, 1); err != nil {
				return err
			}
			// 原子增加热度
			if err := s.videoRepo.ChangePopularity(ctx, tx, videoID, 1); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	// PopularityMQ 投递失败 → 需要同步更新 Redis 热度缓存
	if !popularityMQSent {
		UpdatePopularityCache(ctx, s.cache, videoID, 1)
	}

	return nil
}

// Unlike 取消点赞操作。
// 流程与 Like 镜像对称，change 取负值，计数器用 GREATEST 兜底防负数。
func (s *LikeService) Unlike(ctx context.Context, accountID, videoID uint) error {
	// 参数校验
	if accountID == 0 || videoID == 0 {
		return errors.New("video_id and account_id are required")
	}

	// 确认视频存在
	if s.videoRepo != nil {
		ok, err := s.videoRepo.IsExist(ctx, videoID)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("video not found")
		}
	}

	// === MQ 优先路径 ===
	likeMQSent := false
	popularityMQSent := false

	if s.likeMQ != nil {
		if err := s.likeMQ.Unlike(ctx, accountID, videoID); err == nil {
			likeMQSent = true
		}
	}
	if s.popularityMQ != nil {
		// 热度变化为 -1
		if err := s.popularityMQ.Update(ctx, videoID, -1); err == nil {
			popularityMQSent = true
		}
	}

	// 两条 MQ 都投递成功
	if likeMQSent && popularityMQSent {
		return nil
	}

	// === Fallback：同步 MySQL 事务 ===
	if !likeMQSent {
		err := s.likeRepo.Transaction(ctx, func(tx *gorm.DB) error {
			// 删除点赞记录，未删到行则报错（如用户根本没点过赞）
			if err := s.likeRepo.DeleteByVideoAndAccountInTx(ctx, tx, videoID, accountID); err != nil {
				return err
			}

			// 原子扣减点赞数（GREATEST 兜底防止负数）
			if err := s.videoRepo.ChangeLikesCount(ctx, tx, videoID, -1); err != nil {
				return err
			}
			// 原子扣减热度
			if err := s.videoRepo.ChangePopularity(ctx, tx, videoID, -1); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	// PopularityMQ 投递失败 → 同步更新 Redis
	if !popularityMQSent {
		UpdatePopularityCache(ctx, s.cache, videoID, -1)
	}

	return nil
}

// IsLiked 查询当前用户是否已点赞某视频。
// 直接读 MySQL，无缓存——点赞状态需要实时准确。
func (s *LikeService) IsLiked(ctx context.Context, videoID, accountID uint) (bool, error) {
	return s.likeRepo.IsLiked(ctx, videoID, accountID)
}

// ListLikedVideos 查询当前用户点赞过的所有视频，按点赞时间倒序排列。
func (s *LikeService) ListLikedVideos(ctx context.Context, accountID uint) ([]Video, error) {
	return s.likeRepo.ListLikedVideos(ctx, accountID)
}
