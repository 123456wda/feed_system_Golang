package worker

import (
	"context"
	"errors"

	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/video"
	amqp "github.com/rabbitmq/amqp091-go"
)

type CommentWorker struct {
	queue       string
	rbq         *rabbitmq.RabbitMQ
	commentRepo *video.CommentRepository
	videoRepo   *video.VideoRepository
}

func NewCommentWorker(rbq *rabbitmq.RabbitMQ, commentRepo *video.CommentRepository, videoRepo *video.VideoRepository, queue string) *CommentWorker {
	return &CommentWorker{queue: queue, rbq: rbq, commentRepo: commentRepo, videoRepo: videoRepo}
}

func (w *CommentWorker) Run(ctx context.Context) error {
	if w == nil || w.rbq == nil || w.commentRepo == nil || w.videoRepo == nil {
		return errors.New("comment worker is not initialized")
	}
	if w.queue == "" {
		return errors.New("queue is required")
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

func (w *CommentWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
}
