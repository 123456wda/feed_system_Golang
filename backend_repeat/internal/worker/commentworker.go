package worker

import (
	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/video"
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
