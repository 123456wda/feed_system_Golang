package worker

import (
	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
)

type PopularityWorker struct {
	queue string
	rbq   *rabbitmq.RabbitMQ
	cache *rediscache.Client
}

func NewPopularityWorker(rbq *rabbitmq.RabbitMQ, cache *rediscache.Client, queue string) *PopularityWorker {
	return &PopularityWorker{queue: queue, rbq: rbq, cache: cache}
}
