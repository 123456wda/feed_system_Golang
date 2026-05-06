package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/video"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PopularityWorker 消费 popularityMQ 的消息，负责将热度变更同步到 Redis。
// 点赞/取消点赞/评论等操作会通过 API 投递 PopularityEvent 到 MQ，
// 本 worker 收到后调用 UpdatePopularityCache 更新 Redis 中的热度排行。
type PopularityWorker struct {
	queue string
	rbq   *rabbitmq.RabbitMQ
	cache *rediscache.Client
}

func NewPopularityWorker(rbq *rabbitmq.RabbitMQ, cache *rediscache.Client, queue string) *PopularityWorker {
	return &PopularityWorker{queue: queue, rbq: rbq, cache: cache}
}

// Run 启动消费者循环，阻塞直到 ctx 被取消。
// 从 popularityQueue 中拉取消息，逐条交给 process 处理。
func (w *PopularityWorker) Run(ctx context.Context) error {
	if w == nil || w.rbq == nil || w.cache == nil {
		return errors.New("popularity worker is not initialized")
	}

	// 从已声明的队列中消费消息，autoAck 设为 false 由业务层手动确认
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

	// 无限循环拉取消息，通过 ctx.Done() 实现优雅退出
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
// 成功则 Ack 确认消费，失败则 Nack 并重新入队（不丢弃消息）。
func (w *PopularityWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	if err := w.process(ctx, d.Body); err != nil {
		log.Printf("popularity worker: failed to process message: %v", err)
		// Nack 且 requeue=true：消息重新入队等待重试
		_ = d.Nack(false, true)
		return
	}
	// 确认消费成功，消息从队列中移除
	_ = d.Ack(false)
}

// process 解析 PopularityEvent JSON 并写入 Redis。
// 解析失败或字段无效的消息直接丢弃（不重试，因为重试也会失败）。
func (w *PopularityWorker) process(ctx context.Context, body []byte) error {
	// 解析 JSON 消息体
	var evt rabbitmq.PopularityEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		// JSON 解析失败 → 脏数据，直接丢弃不报错
		return nil
	}

	// 字段校验：videoID 和 change 缺一不可
	if evt.VideoID == 0 || evt.Change == 0 {
		return nil
	}

	// 更新 Redis 热度缓存：
	//   1. 删除视频详情缓存，让下次 GetDetail 读到最新 DB 数据
	//   2. 更新分钟窗口的有序集合，用于 feed 流按热度排行
	video.UpdatePopularityCache(ctx, w.cache, evt.VideoID, evt.Change)
	return nil
}
