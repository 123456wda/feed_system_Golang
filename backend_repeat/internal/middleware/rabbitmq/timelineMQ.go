package rabbitmq

import (
	"context"
	"errors"
	"time"
)

// TimelineMQ 负责视频发布事件的异步投递。
// 生产者：OutboxWorker 轮询 outbox 表后调用 PublishVideo 投递到 MQ。
// 消费者：worker.StartConsumer 消费后写入 Redis ZSET（feed:global_timeline）。
type TimelineMQ struct {
	*RabbitMQ
}

const (
	timelineExchange   = "video.timeline.events"
	timelineQueue      = "video.timeline.update.queue"
	timelineBindingKey = "video.timeline.publish" // 精确匹配 publish 事件，避免收到 fanout 事件
	timelinePublishRK  = "video.timeline.publish"
)

// TimelineEvent 视频发布的时间线事件，由 OutboxWorker 投递。
type TimelineEvent struct {
	EventID    string    `json:"event_id"`
	VideoID    uint      `json:"video_id"`
	CreateTime int64     `json:"create_time"`
	OccurredAt time.Time `json:"occurred_at"`
}

// NewTimelineMQ 创建 TimelineMQ 生产者，声明 topic 交换机和队列。
func NewTimelineMQ(base *RabbitMQ) (*TimelineMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	if err := base.DeclareTopic(timelineExchange, timelineQueue, timelineBindingKey); err != nil {
		return nil, err
	}
	return &TimelineMQ{RabbitMQ: base}, nil
}

// PublishVideo 投递一条视频发布事件到 timeline 交换机。
func (t *TimelineMQ) PublishVideo(ctx context.Context, videoID uint, createTime time.Time) error {
	if t == nil || t.RabbitMQ == nil {
		return errors.New("timeline mq is not initialized")
	}
	if videoID == 0 {
		return errors.New("videoID is required")
	}
	// TODO: 生成事件 ID，组装 TimelineEvent，调用 t.PublishJSON 投递
	id, err := newEventID(16)
	if err != nil {
		return err
	}
	timeline := TimelineEvent{
		EventID:    id,
		VideoID:    videoID,
		CreateTime: createTime.UnixMilli(),
		OccurredAt: time.Now(),
	}
	return t.PublishJSON(ctx, timelineExchange, timelinePublishRK, timeline)
}
