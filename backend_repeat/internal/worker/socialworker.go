package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/social"

	"github.com/go-sql-driver/mysql"
	amqp "github.com/rabbitmq/amqp091-go"
)

// SocialWorker 消费 socialMQ 的消息，负责将关注/取关事件同步写入 MySQL。
// API 的 MQ 投递是"尽力而为"的通知，真正的持久化由本 worker 完成。
// 这样 API 可以快速返回，下游 timeline fanout 等操作被异步化。
type SocialWorker struct {
	rbq   *rabbitmq.RabbitMQ
	repo  *social.SocialRepository
	queue string
}

func NewSocialWorker(rbq *rabbitmq.RabbitMQ, repo *social.SocialRepository, queue string) *SocialWorker {
	return &SocialWorker{rbq: rbq, repo: repo, queue: queue}
}

// Run 启动消费者循环，阻塞直到 ctx 被取消。
func (w *SocialWorker) Run(ctx context.Context) error {
	if w == nil || w.rbq == nil || w.repo == nil {
		return errors.New("social worker is not initialized")
	}

	// 从已声明的队列中消费消息，autoAck=false 由业务层手动控制
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

	// 轮询消费，直到 ctx 被取消或 channel 关闭
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
// 成功 → Ack 确认消费（从队列移除）；失败 → Nack 重新入队等待重试。
func (w *SocialWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	if err := w.process(ctx, d.Body); err != nil {
		log.Printf("social worker: failed to process message: %v", err)
		// Nack + requeue=true：消息重新入队，下次再试
		_ = d.Nack(false, true)
		return
	}
	// 处理成功，确认消费
	_ = d.Ack(false)
}

// process 解析 SocialEvent JSON，根据 Action 字段分发到对应处理逻辑。
// 解析失败的消息直接丢弃（脏数据重试无意义，会一直失败）。
func (w *SocialWorker) process(ctx context.Context, body []byte) error {
	var evt rabbitmq.SocialEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		// JSON 解析失败 → 脏数据，丢弃不重试（return nil 让 handleDelivery Ack 掉）
		return nil
	}
	// 字段校验：关注者和被关注者都不能为 0
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
		if err == nil {
			return nil
		}
		// 处理 MySQL 1062 唯一键冲突（重复投递场景）：
		// MQ 的 at-least-once 语义可能导致同一条消息被多次投递，
		// 第一次插入成功后，后续重复消息会触发 1062，这里幂等忽略。
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return nil
		}
		return err
	case "unfollow":
		// 删除关注记录（Unfollow 本身是幂等的，删 0 行也不报错）
		return w.repo.Unfollow(ctx, &social.Social{
			FollowerID: evt.FollowerID,
			VloggerID:  evt.VloggerID,
		})
	default:
		// 未知事件类型，丢弃
		return nil
	}
}
