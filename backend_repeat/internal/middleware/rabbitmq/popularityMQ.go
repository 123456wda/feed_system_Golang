package rabbitmq

import (
	"errors"
	"time"
)

const (
	popularityExchange   = "video.popularity.events"
	popularityQueue      = "video.popularity.events"
	popularityBindingKey = "video.popularity.*"

	popularityUpdateRK = "video.popularity.update"
)

type PopularityEvent struct {
	EventID    string    `json:"event_id"`
	VideoID    uint      `json:"video_id"`
	Change     int64     `json:"change"`
	OccurredAt time.Time `json:"occurred_at"`
}

type PopularityMQ struct {
	*RabbitMQ
}

func NewPopularityMQ(base *RabbitMQ) (*PopularityMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	if err := base.DeclareTopic(popularityExchange, popularityQueue, popularityBindingKey); err != nil {
		return nil, err
	}
	return &PopularityMQ{RabbitMQ: base}, nil
}
