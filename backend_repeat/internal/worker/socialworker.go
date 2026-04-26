package worker

import (
	"context"
	"errors"

	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/social"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SocialWorker struct {
	rbq   *rabbitmq.RabbitMQ
	repo  *social.SocialRepository
	queue string
}

func NewSocialWorker(rbq *rabbitmq.RabbitMQ, repo *social.SocialRepository, queue string) *SocialWorker {
	return &SocialWorker{rbq: rbq, repo: repo, queue: queue}
}

func (w *SocialWorker) Run(ctx context.Context) error {
	if w == nil || w.rbq == nil || w.repo == nil {
		return errors.New("social worker is not initialized")
	}

	// 启动监听消费
	deliveries, err := w.rbq.Ch.Consume(
		w.queue,
		"",
		false,
		false,
		false,
		false,
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

func (w *SocialWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
}
