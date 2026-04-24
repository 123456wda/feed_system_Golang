package worker

import (
	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/social"
)

type SocialWorker struct {
	rbq   *rabbitmq.RabbitMQ
	repo  *social.SocialRepository
	queue string
}

func NewSocialWorker(rbq *rabbitmq.RabbitMQ, repo *social.SocialRepository, queue string) *SocialWorker {
	return &SocialWorker{rbq: rbq, repo: repo, queue: queue}
}
