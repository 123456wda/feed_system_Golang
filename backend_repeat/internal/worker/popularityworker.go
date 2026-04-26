package worker

import (
	"context"
	"errors"

	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	amqp "github.com/rabbitmq/amqp091-go"
)

type PopularityWorker struct {
	queue string
	rbq   *rabbitmq.RabbitMQ
	cache *rediscache.Client
}

func NewPopularityWorker(rbq *rabbitmq.RabbitMQ, cache *rediscache.Client, queue string) *PopularityWorker {
	return &PopularityWorker{queue: queue, rbq: rbq, cache: cache}
}

func (w *PopularityWorker) Run(ctx context.Context) error {
	if w == nil || w.rbq == nil || w.cache == nil {
		return errors.New("popularity worker is not initialized")
	}

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

func (w *PopularityWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
}
