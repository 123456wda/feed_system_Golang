package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/video"

	amqp "github.com/rabbitmq/amqp091-go"
	"gorm.io/gorm"
)

// LikeWorker 消费 likeMQ 的消息，负责将点赞/取消点赞事件同步写入 MySQL 和 Redis。
// API 的 MQ 优先路径只负责投递消息，真正的持久化由本 worker 完成。
// 这样 API 可以快速返回，写 MySQL 的开销被异步化。
type LikeWorker struct {
	queue     string
	rbq       *rabbitmq.RabbitMQ
	likeRepo  *video.LikeRepository
	videoRepo *video.VideoRepository
}

func NewLikeWorker(rbq *rabbitmq.RabbitMQ, lrepo *video.LikeRepository, vrepo *video.VideoRepository, queue string) *LikeWorker {
	return &LikeWorker{queue: queue, rbq: rbq, likeRepo: lrepo, videoRepo: vrepo}
}

// Run 启动消费者循环，阻塞直到 ctx 被取消。
// 从 likeQueue 中拉取消息，逐条交给 process 处理。
func (w *LikeWorker) Run(ctx context.Context) error {
	if w == nil || w.rbq == nil || w.likeRepo == nil || w.videoRepo == nil {
		return errors.New("like worker is not initialized")
	}

	// 从已声明的队列中消费消息，autoAck=false 由业务层手动控制
	deliveries, err := w.rbq.Ch.Consume(
		w.queue,
		"",
		false, // autoAck: 手动确认
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		return err
	}

	// 开始轮询消费，直到 ctx 被取消或 channel 关闭
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
// 成功 → Ack 确认消费；失败 → Nack 重新入队等待重试。
func (w *LikeWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	if err := w.process(ctx, d.Body); err != nil {
		log.Printf("like worker: failed to process message: %v", err)
		// Nack + requeue，消息重新入队，下次再试
		_ = d.Nack(false, true)
		return
	}
	// 确认消息已被正确处理
	_ = d.Ack(false)
	rabbitmq.IncrConsumed(w.queue)
}

// process 解析 LikeEvent JSON，根据 Action 字段分发到 applyLike 或 applyUnlike。
// 解析失败的消息直接丢弃（脏数据重试无意义）。
func (w *LikeWorker) process(ctx context.Context, body []byte) error {
	// 解析 JSON 消息体为 LikeEvent 结构
	var evt rabbitmq.LikeEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		// JSON 解析失败 → 脏数据，直接丢弃不重试
		return nil
	}

	// 字段校验
	if evt.UserID == 0 || evt.VideoID == 0 {
		return nil
	}

	// 根据 Action 分发到对应处理方法
	switch evt.Action {
	case "like":
		return w.applyLike(ctx, evt.UserID, evt.VideoID)
	case "unlike":
		return w.applyUnlike(ctx, evt.UserID, evt.VideoID)
	default:
		// 未知事件类型，丢弃
		return nil
	}
}

// applyLike 处理点赞事件：检查视频存在 → 幂等插入点赞记录 → 更新计数 + 热度。
// 使用 LikeIgnoreDuplicateInTx 保证幂等性：
//   - 重复点赞消息 → created=false → 不更新计数，直接返回
//   - 首次点赞消息 → created=true → 更新点赞数和热度
func (w *LikeWorker) applyLike(ctx context.Context, userID, videoID uint) error {
	// 确认视频存在，若视频已删除则丢弃该消息
	ok, err := w.videoRepo.IsExist(ctx, videoID)
	if err != nil {
		return err
	}
	if !ok {
		// 视频不存在 → 丢弃消息，不重试
		return nil
	}

	return w.likeRepo.Transaction(ctx, func(tx *gorm.DB) error {
		// 幂等插入点赞记录，遇 1062 重复键返回 created=false
		created, err := w.likeRepo.LikeIgnoreDuplicateInTx(ctx, tx, &video.Like{
			VideoID:   videoID,
			AccountID: userID,
			CreatedAt: time.Now(),
		})
		if err != nil {
			return err
		}
		if !created {
			// 点赞记录已存在（重复消息），跳过计数更新
			return nil
		}

		// 只有真正创建了新记录才更新计数
		// 原子增加点赞数
		if err := w.videoRepo.ChangeLikesCount(ctx, tx, videoID, 1); err != nil {
			return err
		}
		// 原子增加热度
		return w.videoRepo.ChangePopularity(ctx, tx, videoID, 1)
	})
}

// applyUnlike 处理取消点赞事件：检查视频存在 → 删除点赞记录 → 扣减计数 + 热度。
// 使用 DeleteByVideoAndAccountTx 的 deleted 返回值判断是否真的删除了记录：
//   - 重复取消点赞消息 → deleted=false → 跳过计数更新
//   - 首次取消点赞消息 → deleted=true → 扣减点赞数和热度
// 事务保证删除记录和扣减计数的原子性，避免部分成功导致数据不一致。
func (w *LikeWorker) applyUnlike(ctx context.Context, userID, videoID uint) error {
	// 确认视频存在，若视频已删除则丢弃该消息
	ok, err := w.videoRepo.IsExist(ctx, videoID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	return w.likeRepo.Transaction(ctx, func(tx *gorm.DB) error {
		// 删除点赞记录，返回 deleted 表示是否真的删到了行
		deleted, err := w.likeRepo.DeleteByVideoAndAccountTx(ctx, tx, videoID, userID)
		if err != nil {
			return err
		}
		if !deleted {
			// 记录本就不存在（重复消息或根本没点过赞），跳过计数更新
			return nil
		}

		// 只有真正删除了记录才扣减计数（GREATEST 兜底防止负数）
		if err := w.videoRepo.ChangeLikesCount(ctx, tx, videoID, -1); err != nil {
			return err
		}
		return w.videoRepo.ChangePopularity(ctx, tx, videoID, -1)
	})
}
