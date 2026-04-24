package worker

import (
	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/video"
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
