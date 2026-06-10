package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"

	"github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	amqp "github.com/rabbitmq/amqp091-go"
)

// SocialWorker 消费 socialMQ 的消息，负责将关注/取关事件同步写入 MySQL。
// 同时维护推拉结合所需的 Redis 数据结构：
//   - 关注大V时，将 vloggerID 加入 following:bigv:{followerID} SET
//   - 关注普通用户时，回填被关注者最近视频到 inbox:{followerID} ZSET
//   - 取关时，从 following:bigv:{followerID} SET 移除
type SocialWorker struct {
	rbq   *rabbitmq.RabbitMQ
	repo  *social.SocialRepository
	cache *rediscache.Client // 用于维护推拉结合的 Redis 数据结构
	queue string
}

func NewSocialWorker(rbq *rabbitmq.RabbitMQ, repo *social.SocialRepository, cache *rediscache.Client, queue string) *SocialWorker {
	return &SocialWorker{rbq: rbq, repo: repo, cache: cache, queue: queue}
}

// Run 启动消费者循环，阻塞直到 ctx 被取消。
func (w *SocialWorker) Run(ctx context.Context) error {
	if w == nil || w.rbq == nil || w.repo == nil {
		return errors.New("social worker is not initialized")
	}

	deliveries, err := w.rbq.Ch.Consume(
		w.queue,
		"",    // consumer: 空字符串由 RabbitMQ 自动生成
		false, // autoAck: 手动确认，处理成功才 Ack
		false, // exclusive: 不独占队列
		false, // noLocal: 不拒绝本连接发布的消息
		false, // noWait: 等待服务器确认
		nil,
	)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("deliveries channel closed")
			}
			w.handleDelivery(ctx, d)
		}
	}
}

// handleDelivery 处理单条 RabbitMQ 投递。
func (w *SocialWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	if err := w.process(ctx, d.Body); err != nil {
		log.Printf("social worker: failed to process message: %v", err)
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
	rabbitmq.IncrConsumed(w.queue)
}

// process 解析 SocialEvent JSON，根据 Action 字段分发到对应处理逻辑。
func (w *SocialWorker) process(ctx context.Context, body []byte) error {
	var evt rabbitmq.SocialEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return nil
	}
	if evt.FollowerID == 0 || evt.VloggerID == 0 {
		return nil
	}

	switch evt.Action {
	case "follow":
		// 插入关注记录
		err := w.repo.Follow(ctx, &social.Social{
			FollowerID: evt.FollowerID,
			VloggerID:  evt.VloggerID,
		})
		if err != nil {
			var mysqlErr *mysql.MySQLError
			if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
				// 幂等忽略重复投递
			} else {
				return err
			}
		}
		// 关注成功后，维护推拉结合的 Redis 数据结构
		w.onFollow(ctx, evt.FollowerID, evt.VloggerID)
		return nil
	case "unfollow":
		err := w.repo.Unfollow(ctx, &social.Social{
			FollowerID: evt.FollowerID,
			VloggerID:  evt.VloggerID,
		})
		// 取关后清理 bigv SET
		w.onUnfollow(ctx, evt.FollowerID, evt.VloggerID)
		return err
	default:
		return nil
	}
}

// onFollow 关注成功后的 Redis 维护逻辑。
func (w *SocialWorker) onFollow(ctx context.Context, followerID, vloggerID uint) {
	if w.cache == nil {
		return
	}
	opCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// 查询被关注者粉丝数，判断是否大V
	isBigV, _ := w.isBigV(opCtx, vloggerID)

	bigvKey := fmt.Sprintf("following:bigv:%d", followerID)
	if isBigV {
		// 关注的是大V，加入 bigv SET
		_ = w.cache.SAdd(opCtx, bigvKey, fmt.Sprintf("%d", vloggerID))
		_ = w.cache.Expire(opCtx, bigvKey, 24*time.Hour)
	} else {
		// 关注的是普通用户，回填被关注者最近视频到 inbox
		w.backfillInbox(opCtx, followerID, vloggerID)
	}
}

// onUnfollow 取关后的 Redis 维护逻辑。
func (w *SocialWorker) onUnfollow(_ context.Context, followerID, vloggerID uint) {
	if w.cache == nil {
		return
	}
	opCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// 从 bigv SET 移除（无论是否大V，SRem 幂等）
	bigvKey := fmt.Sprintf("following:bigv:%d", followerID)
	_ = w.cache.SRem(opCtx, bigvKey, fmt.Sprintf("%d", vloggerID))
}

// isBigV 判断用户是否大V（粉丝数 >= BigVThreshold）。
func (w *SocialWorker) isBigV(ctx context.Context, userID uint) (bool, error) {
	cacheKey := fmt.Sprintf("user:follower_count:%d", userID)

	// 尝试从 Redis 读取粉丝数
	val, err := w.cache.GetBytes(ctx, cacheKey)
	if err == nil {
		count, parseErr := strconv.ParseInt(string(val), 10, 64)
		if parseErr == nil {
			return count >= BigVThreshold, nil
		}
	}

	// 缓存未命中，查 MySQL
	count, err := w.repo.CountFollowers(ctx, userID)
	if err != nil {
		return false, err
	}
	// 回写 Redis 缓存
	_ = w.cache.SetBytes(ctx, cacheKey, []byte(fmt.Sprintf("%d", count)), 0)
	return count >= BigVThreshold, nil
}

// backfillInbox 回填被关注者最近视频到关注者的收件箱。
// 从 user_videos:{vloggerID} ZSET 取最近 50 条，写入 inbox:{followerID} ZSET。
func (w *SocialWorker) backfillInbox(ctx context.Context, followerID, vloggerID uint) {
	outboxKey := fmt.Sprintf("user_videos:%d", vloggerID)
	inboxKey := fmt.Sprintf("inbox:%d", followerID)

	// 从被关注者的 outbox 取最近 50 条视频
	videos, err := w.cache.ZRevRangeWithScores(ctx, outboxKey, 0, 49)
	if err != nil || len(videos) == 0 {
		return
	}

	// 批量写入关注者的 inbox
	rdb := w.cache.GetRedisClient()
	if rdb != nil && !w.cache.IsBreakerOpen() {
		pipe := rdb.Pipeline()
		for _, v := range videos {
			pipe.ZAdd(ctx, inboxKey, redis.Z{
				Member: v.Member,
				Score:  v.Score,
			})
		}
		// 裁剪到 inboxCap
		pipe.ZRemRangeByRank(ctx, inboxKey, 0, -(inboxCap + 1))
		_, _ = pipe.Exec(ctx)
	} else {
		// Pipeline 不可用，逐个写入
		for _, v := range videos {
			_ = w.cache.ZAdd(ctx, inboxKey, redis.Z{
				Member: v.Member,
				Score:  v.Score,
			})
		}
		_ = w.cache.ZRemRangeByRank(ctx, inboxKey, 0, -(inboxCap + 1))
	}
}
