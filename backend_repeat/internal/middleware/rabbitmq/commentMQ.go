package rabbitmq

import (
	"context"
	"errors"
	"time"
)

type CommentMQ struct {
	*RabbitMQ
}

const (
	commentExchange   = "comment.events"
	commentQueue      = "comment.events"
	commentBindingKey = "comment.*"

	commentPublishRK = "comment.publish"
	commentDeleteRK  = "comment.delete"
)

// CommentEvent 是评论事件的传输载体，MQ 生产者和消费者共用这个结构。
type CommentEvent struct {
	EventID    string    `json:"event_id"`
	Action     string    `json:"action"`
	CommentID  uint      `json:"comment_id,omitempty"`
	Username   string    `json:"username,omitempty"`
	VideoID    uint      `json:"video_id,omitempty"`
	AuthorID   uint      `json:"author_id,omitempty"`
	Content    string    `json:"content,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
}

func NewCommentMQ(base *RabbitMQ) (*CommentMQ, error) {
	if base == nil {
		return nil, errors.New("base is nil")
	}
	if err := base.DeclareTopic(commentExchange, commentQueue, commentBindingKey); err != nil {
		return nil, err
	}
	return &CommentMQ{base}, nil
}

// Publish 投递"发布评论"事件，消费者收到后写入 MySQL 并更新热度。
func (c *CommentMQ) Publish(ctx context.Context, username string, videoID, authorID uint, content string) error {
	return c.publish(ctx, "publish", commentPublishRK, CommentEvent{
		Username: username,
		VideoID:  videoID,
		AuthorID: authorID,
		Content:  content,
	})
}

// Delete 投递"删除评论"事件，消费者收到后删除评论记录。
func (c *CommentMQ) Delete(ctx context.Context, commentID uint) error {
	return c.publish(ctx, "delete", commentDeleteRK, CommentEvent{
		CommentID: commentID,
	})
}

// publish 是底层投递方法，自动填充 EventID、Action、OccurredAt 后序列化为 JSON 发到 exchange。
func (c *CommentMQ) publish(ctx context.Context, action, routingKey string, event CommentEvent) error {
	if c == nil || c.RabbitMQ == nil {
		return errors.New("comment mq is not initialized")
	}
	id, err := newEventID(16)
	if err != nil {
		return err
	}
	event.EventID = id
	event.Action = action
	event.OccurredAt = time.Now().UTC()
	return c.PublishJSON(ctx, commentExchange, routingKey, event)
}
