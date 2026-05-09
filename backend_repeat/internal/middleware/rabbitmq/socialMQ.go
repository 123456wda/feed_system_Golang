package rabbitmq

import (
	"context"
	"errors"
	"time"
)

// SocialMQ 封装关注/取关事件的 RabbitMQ 生产者。
// 内部持有通用 RabbitMQ 连接，使用 Topic Exchange 实现事件广播：
//   - API 进程（生产者）通过 Follow/UnFollow 方法投递事件
//   - Worker 进程（消费者）从 social.events 队列消费，写入 timeline 等下游数据
//
// 关注关系本身由 API 同步写入 MySQL，MQ 只负责通知下游（如 timeline fanout），
// 因此 MQ 投递失败不影响核心业务，属于"尽力而为"的异步通知。
type SocialMQ struct {
	*RabbitMQ
}

// RabbitMQ 拓扑常量：exchange、queue、binding key 以及各操作的 routing key。
const (
	socialExchange   = "social.events" // Topic Exchange 名称
	socialQueue      = "social.events" // 消费者队列名称
	socialBindingKey = "social.*"      // 绑定所有 social.* 路由

	socialFollowRK   = "social.follow"   // 关注事件的 routing key
	socialUnfollowRK = "social.unfollow" // 取关事件的 routing key
)

// SocialEvent 是关注/取关事件的 JSON 传输载体。
// 生产者序列化后发到 exchange，消费者反序列化后处理。
type SocialEvent struct {
	EventID    string    `json:"event_id"`    // 事件唯一 ID，用于去重/追踪
	Action     string    `json:"action"`      // 操作类型："follow" 或 "unfollow"
	FollowerID uint      `json:"follower_id"` // 关注者（粉丝）的账号 ID
	VloggerID  uint      `json:"vlogger_id"`  // 被关注者（博主）的账号 ID
	OccurredAt time.Time `json:"occurred_at"` // 事件发生时间（UTC）
}

// NewSocialMQ 创建 SocialMQ 生产者实例。
// 内部会自动声明 exchange + queue + binding，确保拓扑存在。
func NewSocialMQ(base *RabbitMQ) (*SocialMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	// 声明 Topic Exchange 和队列，幂等操作（已存在则跳过）
	if err := base.DeclareTopic(socialExchange, socialQueue, socialBindingKey); err != nil {
		return nil, err
	}
	return &SocialMQ{RabbitMQ: base}, nil
}

// Follow 投递"关注"事件到 MQ。
// 消费者收到后会写入 timeline fanout，让粉丝在信息流中看到博主的新内容。
func (s *SocialMQ) Follow(ctx context.Context, followerID, vloggerID uint) error {
	return s.publish(ctx, "follow", socialFollowRK, followerID, vloggerID)
}

// UnFollow 投递"取关"事件到 MQ。
// 消费者收到后会清理 timeline 中该博主的内容。
func (s *SocialMQ) UnFollow(ctx context.Context, followerID, vloggerID uint) error {
	return s.publish(ctx, "unfollow", socialUnfollowRK, followerID, vloggerID)
}

// publish 是底层投递方法。
// 自动填充 EventID（随机 16 字节 hex）和 OccurredAt（当前 UTC 时间），
// 然后序列化为 JSON 发到 social.events exchange。
func (s *SocialMQ) publish(ctx context.Context, action, routingKey string, followerID, vloggerID uint) error {
	// 防御：nil 检查
	if s == nil || s.RabbitMQ == nil {
		return errors.New("social mq is not initialized")
	}
	// 参数校验：关注者和被关注者都不能为 0
	if followerID == 0 || vloggerID == 0 {
		return errors.New("followerID and vloggerID are required")
	}
	// 生成随机事件 ID，用于消息去重和链路追踪
	id, err := newEventID(16)
	if err != nil {
		return err
	}
	// 组装事件结构体
	evt := SocialEvent{
		EventID:    id,
		Action:     action,
		FollowerID: followerID,
		VloggerID:  vloggerID,
		OccurredAt: time.Now().UTC(),
	}
	// 序列化为 JSON 并投递到 exchange
	return s.PublishJSON(ctx, socialExchange, routingKey, evt)
}
