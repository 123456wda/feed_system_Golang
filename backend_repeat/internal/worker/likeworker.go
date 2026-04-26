package worker

import (
	"context"
	"errors"

	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/video"
	amqp "github.com/rabbitmq/amqp091-go"
)

type LikeWorker struct {
	queue     string
	rbq       *rabbitmq.RabbitMQ
	likeRepo  *video.LikeRepository
	videoRepo *video.VideoRepository
}

func NewLikeWorker(rbq *rabbitmq.RabbitMQ, lrepo *video.LikeRepository, vrepo *video.VideoRepository, queue string) *LikeWorker {
	return &LikeWorker{queue: queue, rbq: rbq, likeRepo: lrepo, videoRepo: vrepo}
}

func (w *LikeWorker) Run(ctx context.Context) error {
	if w == nil || w.rbq == nil || w.likeRepo == nil || w.videoRepo == nil {
		return errors.New("like worker is not initialized")
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

	// 开始轮询消费
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

func (w *LikeWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
}
