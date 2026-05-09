package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/video"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// StartOutboxPoller 启动 outbox 轮询器（一个 goroutine）。
// 不断从 outbox 表中取 status='pending' 的记录，投递到 TimelineMQ，成功后删除。
// 保证"视频写入 DB"和"事件投递到 MQ"的最终一致性。
func StartOutboxPoller(db *gorm.DB, tmq *rabbitmq.TimelineMQ) {
	// TODO: 启动 goroutine，轮询 outbox 表
	// 1. SELECT * FROM outbox_msgs WHERE status='pending' ORDER BY create_time ASC LIMIT 100
	// 2. 逐条调用 tmq.PublishVideo()
	// 3. 投递成功则 DELETE 该条记录
	// 4. 失败则 log 记录，下次重试
	// 5. 无记录时 sleep 1 秒

	go func() {
		for {
			var msgs []video.OutboxMsg
			err := db.Where("status = ?", "pending").Order("create_time ASC").Find(&msgs).Error
			if err != nil || len(msgs) == 0 {
				time.Sleep(1 * time.Second)
				continue
			}
			for _, msg := range msgs {
				err := tmq.PublishVideo(context.Background(), msg.VideoID, msg.CreateTime)
				if err != nil {
					log.Printf("投递MQ失败: VideoID: %d, err: %v", msg.VideoID, err)
				} else {
					db.Delete(&msg)
				}
			}

		}
	}()
}

// StartConsumer 启动 timeline 消费者（一个 goroutine）。
// 消费 TimelineMQ 消息，将视频 ID 写入 Redis ZSET (feed:global_timeline)。
// ZSET score = 视频创建时间毫秒时间戳，ZREMRANGEBYRANK 保持只保留最新 1000 条。
func StartConsumer(tmq *rabbitmq.TimelineMQ, queueName string, redisClient *rediscache.Client) {
	// TODO: 注册消费者，循环消费消息
	// 1. tmq.Ch.Consume(queueName, ...) 注册消费者
	// 2. for msg := range msgs 循环处理
	// 3. 反序列化 TimelineEvent
	// 4. ZADD feed:global_timeline score=create_time member=video_id
	// 5. ZREMRANGEBYRANK 0 -1001 裁剪，只保留最新 1000 条
	// 6. 成功则 msg.Ack，失败则 msg.Nack 重试
	msgs, err := tmq.Ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		log.Printf("注册消费者失败")
		return
	}
	// msgs是一个管道
	go func() {
		for msg := range msgs {
			var val rabbitmq.TimelineEvent
			err := json.Unmarshal(msg.Body, &val)
			if err != nil {
				log.Printf("反序列化失败")
				msg.Nack(false, true)
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			timelineKey := "feed:global_timeline"
			err = redisClient.ZAdd(ctx, timelineKey, redis.Z{
				Member: fmt.Sprintf("%d", val.VideoID),
				Score:  float64(val.CreateTime),
			})
			if err != nil {
				log.Printf("写入Zset失败")
				msg.Nack(false, true)
				cancel()
				continue
			}
			err = redisClient.ZRemRangeByRank(ctx, timelineKey, 0, -1001)
			if err != nil {
				log.Printf("ZRem失败")
			}
			msg.Ack(false)
		}
	}()
}
