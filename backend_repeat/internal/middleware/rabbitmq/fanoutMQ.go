package rabbitmq

import (
	"context"
	"errors"
	"time"
)

// FanoutMQ 负责视频发布后的粉丝收件箱推送（推拉结合中的推路径）。
// 生产者：OutboxWorker 轮询 outbox 表后调用 PublishFanout 投递到 MQ。
// 消费者：FanoutWorker 消费后根据作者粉丝数决定推/拉策略：
//   - 普通用户（粉丝 < 阈值）：遍历粉丝，写入 inbox:{followerID} ZSET
//   - 大V（粉丝 >= 阈值）：不 fanout，只写 user_videos:{authorID} ZSET
//
// 复用 TimelineMQ 的同一个 Topic Exchange (video.timeline.events)，
// 使用独立 routing key 和队列，实现发布-订阅分离。
type FanoutMQ struct {
	*RabbitMQ
}

const (
	// 复用 timeline exchange，独立 routing key 和队列
	fanoutQueue      = "video.timeline.fanout.queue"
	fanoutBindingKey = "video.timeline.fanout"
	fanoutPublishRK  = "video.timeline.fanout"
)

// FanoutEvent 视频 fanout 事件，比 TimelineEvent 多了 AuthorID 字段。
type FanoutEvent struct {
	EventID    string    `json:"event_id"`
	VideoID    uint      `json:"video_id"`
	AuthorID   uint      `json:"author_id"`  // 视频作者 ID，用于判断是否大V
	CreateTime int64     `json:"create_time"` // 视频创建时间毫秒时间戳
	OccurredAt time.Time `json:"occurred_at"` // 事件发生时间
}

// NewFanoutMQ 创建 FanoutMQ 生产者实例。
// 内部声明独立队列并绑定到 timeline exchange 的 fanout routing key。
func NewFanoutMQ(base *RabbitMQ) (*FanoutMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	// 声明独立队列，绑定到已有的 timeline exchange
	if err := base.DeclareTopic(timelineExchange, fanoutQueue, fanoutBindingKey); err != nil {
		return nil, err
	}
	return &FanoutMQ{RabbitMQ: base}, nil
}

// PublishFanout 投递一条 fanout 事件到 timeline 交换机。
func (f *FanoutMQ) PublishFanout(ctx context.Context, videoID uint, authorID uint, createTime time.Time) error {
	if f == nil || f.RabbitMQ == nil {
		return errors.New("fanout mq is not initialized")
	}
	if videoID == 0 || authorID == 0 {
		return errors.New("videoID and authorID are required")
	}
	id, err := newEventID(16)
	if err != nil {
		return err
	}
	evt := FanoutEvent{
		EventID:    id,
		VideoID:    videoID,
		AuthorID:   authorID,
		CreateTime: createTime.UnixMilli(),
		OccurredAt: time.Now(),
	}
	return f.PublishJSON(ctx, timelineExchange, fanoutPublishRK, evt)
}
