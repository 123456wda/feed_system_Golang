package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"

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

// handleDelivery 处理单条 RabbitMQ 投递。
// 成功 → Ack 确认消费；失败 → Nack 重新入队等待重试。
func (w *CommentWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	if err := w.process(ctx, d.Body); err != nil {
		log.Printf("comment worker: failed to process message: %v", err)
		// Nack + requeue，消息重新入队，下次再试
		_ = d.Nack(false, true)
		return
	}
	// 确认消息已被正确处理
	_ = d.Ack(false)
	rabbitmq.IncrConsumed(w.queue)
}

// process 解析 CommentEvent JSON，根据 Action 字段分发到 applyPublish 或 applyDelete。
// JSON 解析失败直接丢弃（脏数据重试无意义）。
func (w *CommentWorker) process(ctx context.Context, body []byte) error {
	var evt rabbitmq.CommentEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		// 解析失败 → 脏数据，丢弃不重试
		return nil
	}

	switch evt.Action {
	case "publish":
		// publish 消息校验：必须有 username、video_id、author_id、content
		if evt.Username == "" || evt.VideoID == 0 || evt.AuthorID == 0 || strings.TrimSpace(evt.Content) == "" {
			return nil
		}
		return w.applyPublish(ctx, &evt)
	case "delete":
		// delete 消息校验：必须有 comment_id
		if evt.CommentID == 0 {
			return nil
		}
		return w.applyDelete(ctx, &evt)
	default:
		return nil
	}
}

// applyPublish 处理发布评论事件：校验视频存在 → 写入评论 → 更新热度。
func (w *CommentWorker) applyPublish(ctx context.Context, evt *rabbitmq.CommentEvent) error {
	// 确认视频存在，视频已删除则丢弃消息
	ok, err := w.videoRepo.IsExist(ctx, evt.VideoID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	c := &video.Comment{
		Username: strings.TrimSpace(evt.Username),
		VideoID:  evt.VideoID,
		AuthorID: evt.AuthorID,
		Content:  strings.TrimSpace(evt.Content),
	}
	if err := w.commentRepo.CreateComment(ctx, c); err != nil {
		return err
	}
	// 写评论成功后原子增加视频热度
	return w.videoRepo.ChangePopularity(ctx, nil, evt.VideoID, 1)
}

// applyDelete 处理删除评论事件：根据 ID 查询评论 → 删除。
func (w *CommentWorker) applyDelete(ctx context.Context, evt *rabbitmq.CommentEvent) error {
	comment, err := w.commentRepo.GetByID(ctx, evt.CommentID)
	if err != nil {
		return err
	}
	if comment == nil {
		// 评论已不存在（重复删除消息），幂等返回
		return nil
	}
	return w.commentRepo.DeleteComment(ctx, comment)
}
