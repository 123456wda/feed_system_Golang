package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"

	"github.com/redis/go-redis/v9"
	amqp "github.com/rabbitmq/amqp091-go"
)

// BigVThreshold 大V粉丝数阈值，超过此数的作者不执行 fanout（推拉结合中的拉路径）。
// 生产环境建议通过 Redis 配置中心动态调整。
const BigVThreshold int64 = 10000

// inboxCap 收件箱最大容量，超出时裁剪最旧的条目。
const inboxCap int64 = 500

// userVideosCap 作者发件箱最大容量。
const userVideosCap int64 = 50

// fanoutBatchSize 每批处理的粉丝数量，参考 Stream-Framework 的 chunk 模式。
const fanoutBatchSize = 100

// trimChance 概率裁剪概率（1%），参考 Stream-Framework 的 probabilistic trim 策略。
// 每次写入 inbox 时以 1% 概率触发 ZREMRANGEBYRANK 裁剪，避免每次写入都裁剪的开销。
const trimChance = 0.01

// FanoutWorker 消费 FanoutMQ 消息，实现推拉结合中的推路径。
// 根据作者粉丝数决定策略：
//   - 普通用户（粉丝 < 阈值）：遍历粉丝，写入 inbox:{followerID} ZSET
//   - 大V（粉丝 >= 阈值）：不 fanout，只写 user_videos:{authorID} ZSET
type FanoutWorker struct {
	rbq        *rabbitmq.RabbitMQ
	socialRepo *social.SocialRepository
	cache      *rediscache.Client
	queue      string
}

// NewFanoutWorker 创建 FanoutWorker 实例。
func NewFanoutWorker(rbq *rabbitmq.RabbitMQ, socialRepo *social.SocialRepository, cache *rediscache.Client, queue string) *FanoutWorker {
	return &FanoutWorker{rbq: rbq, socialRepo: socialRepo, cache: cache, queue: queue}
}

// Run 启动消费者循环，阻塞直到 ctx 被取消。
func (w *FanoutWorker) Run(ctx context.Context) error {
	if w == nil || w.rbq == nil || w.socialRepo == nil || w.cache == nil {
		return errors.New("fanout worker is not initialized")
	}

	deliveries, err := w.rbq.Ch.Consume(
		w.queue,
		"",    // consumer: 空字符串由 RabbitMQ 自动生成
		false, // autoAck: 手动确认，处理成功才 Ack
		false, // exclusive: 不独占队列
		false, // noLocal: 不拒绝本连接发布的消息
		false, // noWait: 等待服务器确认
		nil,
	)
	if err != nil {
		return err
	}

	// 轮询消费，直到 ctx 被取消或 channel 关闭
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
func (w *FanoutWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	if err := w.process(ctx, d.Body); err != nil {
		log.Printf("fanout worker: failed to process message: %v", err)
		if errors.Is(err, rediscache.ErrBreakerOpen) {
			time.Sleep(time.Second)
		}
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
	rabbitmq.IncrConsumed(w.queue)
}

// process 解析 FanoutEvent JSON，执行推拉结合的推路径逻辑。
func (w *FanoutWorker) process(_ context.Context, body []byte) error {
	var evt rabbitmq.FanoutEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		// JSON 解析失败 → 脏数据，丢弃不重试
		return nil
	}
	if evt.VideoID == 0 || evt.AuthorID == 0 {
		return nil
	}

	// 独立超时 context，避免被请求 context 取消连带
	opCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 第一步：写 user_videos:{authorID} ZSET（所有作者都写，作为发件箱/拉路径数据源）
	videoIDStr := fmt.Sprintf("%d", evt.VideoID)
	score := float64(evt.CreateTime)
	outboxKey := fmt.Sprintf("user_videos:%d", evt.AuthorID)

	if err := w.cache.ZAdd(opCtx, outboxKey, redis.Z{
		Member: videoIDStr,
		Score:  score,
	}); err != nil {
		log.Printf("fanout worker: 写 outbox 失败 %s: %v", outboxKey, err)
		return err
	}
	// 裁剪 outbox 到 userVideosCap
	_ = w.cache.ZRemRangeByRank(opCtx, outboxKey, 0, -(userVideosCap + 1))
	// 设置 TTL
	_ = w.cache.Expire(opCtx, outboxKey, 24*time.Hour)

	// 第二步：查询作者粉丝数，判断是否大V
	followerCount, err := w.getFollowerCount(opCtx, evt.AuthorID)
	if err != nil {
		log.Printf("fanout worker: 查询粉丝数失败 AuthorID=%d: %v", evt.AuthorID, err)
		// 查询失败时保守处理：不 fanout，只写了 outbox
		return nil
	}

	// 大V不 fanout，内容只在 outbox 中，由读路径拉取
	if followerCount >= BigVThreshold {
		return nil
	}

	// 第三步：普通用户，执行 fanout 推送到活跃粉丝收件箱
	followerIDs, err := w.socialRepo.GetAllFollowerIDs(opCtx, evt.AuthorID)
	if err != nil {
		log.Printf("fanout worker: 查询粉丝列表失败 AuthorID=%d: %v", evt.AuthorID, err)
		return err
	}
	if len(followerIDs) == 0 {
		return nil
	}

	// 过滤出 3 天内登录过的活跃粉丝，僵尸粉不推，走拉路径兜底
	followerIDs = w.filterActiveFollowers(opCtx, followerIDs)
	if len(followerIDs) == 0 {
		return nil
	}

	// 分批处理，参考 Stream-Framework 的 chunk fanout 模式
	for i := 0; i < len(followerIDs); i += fanoutBatchSize {
		end := i + fanoutBatchSize
		if end > len(followerIDs) {
			end = len(followerIDs)
		}
		batch := followerIDs[i:end]
		w.fanoutBatch(opCtx, batch, videoIDStr, score)
	}

	return nil
}

// fanoutBatch 使用 Redis Pipeline 批量写入一批粉丝的收件箱。
// 参考 Stream-Framework 的 pipeline batch 模式，减少 Redis RTT。
func (w *FanoutWorker) fanoutBatch(ctx context.Context, followerIDs []uint, videoIDStr string, score float64) {
	rdb := w.cache.GetRedisClient()
	if rdb == nil || w.cache.IsBreakerOpen() {
		for _, fid := range followerIDs {
			inboxKey := fmt.Sprintf("inbox:%d", fid)
			_ = w.cache.ZAdd(ctx, inboxKey, redis.Z{Member: videoIDStr, Score: score})
			w.maybeTrimInbox(ctx, inboxKey)
		}
		return
	}

	// 使用 Pipeline 批量写入，一次网络往返完成所有 ZADD
	pipe := rdb.Pipeline()
	for _, fid := range followerIDs {
		inboxKey := fmt.Sprintf("inbox:%d", fid)
		pipe.ZAdd(ctx, inboxKey, redis.Z{Member: videoIDStr, Score: score})
		// 概率裁剪：1% 概率触发 ZREMRANGEBYRANK，避免每次写入都裁剪
		if rand.Float64() < trimChance {
			pipe.ZRemRangeByRank(ctx, inboxKey, 0, -(inboxCap + 1))
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("fanout worker: pipeline 执行失败: %v", err)
	}
}

// maybeTrimInbox 概率裁剪收件箱，1% 概率触发 ZREMRANGEBYRANK。
func (w *FanoutWorker) maybeTrimInbox(ctx context.Context, inboxKey string) {
	if rand.Float64() < trimChance {
		_ = w.cache.ZRemRangeByRank(ctx, inboxKey, 0, -(inboxCap + 1))
	}
}

// filterActiveFollowers 用 Redis Pipeline 批量检查 user:active:{id} key 是否存在，
// 只返回 3 天内登录过的活跃粉丝 ID。不存在的视为僵尸粉，不推收件箱，走拉路径兜底。
func (w *FanoutWorker) filterActiveFollowers(ctx context.Context, allIDs []uint) []uint {
	rdb := w.cache.GetRedisClient()
	if rdb == nil || w.cache.IsBreakerOpen() {
		return allIDs
	}

	pipe := rdb.Pipeline()
	cmds := make(map[uint]*redis.IntCmd, len(allIDs))
	for _, id := range allIDs {
		key := fmt.Sprintf("user:active:%d", id)
		cmds[id] = pipe.Exists(ctx, key)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		log.Printf("fanout worker: Pipeline Exists 失败: %v", err)
		return allIDs
	}

	active := make([]uint, 0, len(allIDs))
	for _, id := range allIDs {
		if cmds[id].Val() > 0 {
			active = append(active, id)
		}
	}
	return active
}

// getFollowerCount 查询用户粉丝数。
// 优先从 Redis 缓存读取，缓存未命中时查 MySQL 并回写 Redis。
func (w *FanoutWorker) getFollowerCount(ctx context.Context, userID uint) (int64, error) {
	cacheKey := fmt.Sprintf("user:follower_count:%d", userID)

	// 尝试从 Redis 读取
	val, err := w.cache.GetBytes(ctx, cacheKey)
	if err == nil {
		count, parseErr := strconv.ParseInt(string(val), 10, 64)
		if parseErr == nil {
			return count, nil
		}
	}

	// 缓存未命中或解析失败，查 MySQL（social 表 COUNT）
	count, err := w.socialRepo.CountFollowers(ctx, userID)
	if err != nil {
		return 0, err
	}
	// 回写 Redis 缓存（不设 TTL，通过关注/取关时更新）
	_ = w.cache.SetBytes(ctx, cacheKey, []byte(fmt.Sprintf("%d", count)), 0)

	return count, nil
}
