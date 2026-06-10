package redis

import (
	"context"
	"errors"
	"time"

	"feedsystem_video_go/internal/observability"

	"github.com/redis/go-redis/v9"
)

// MGet 批量获取多个 key 的值，用于 feed 流批量查询视频缓存。
func (c *Client) MGet(ctx context.Context, keys ...string) ([]interface{}, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	var vals []interface{}
	err := c.breaker.Execute(func() error {
		start := time.Now()
		v, e := c.rdb.MGet(ctx, keys...).Result()
		dur := time.Since(start).Seconds()
		if e != nil {
			observability.RedisOperationsTotal.WithLabelValues("mget", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("mget", "error").Observe(dur)
			return e
		}
		observability.RedisOperationsTotal.WithLabelValues("mget", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("mget", "success").Observe(dur)
		vals = v
		return nil
	})
	return vals, err
}

// ZAdd 向有序集合添加成员。
func (c *Client) ZAdd(ctx context.Context, key string, members ...redis.Z) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.breaker.Execute(func() error {
		start := time.Now()
		err := c.rdb.ZAdd(ctx, key, members...).Err()
		dur := time.Since(start).Seconds()
		if err != nil {
			observability.RedisOperationsTotal.WithLabelValues("zadd", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("zadd", "error").Observe(dur)
			return err
		}
		observability.RedisOperationsTotal.WithLabelValues("zadd", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("zadd", "success").Observe(dur)
		return nil
	})
}

// ZRemRangeByRank 移除有序集合中指定排名范围的成员。
// 用于 timeline ZSET 裁剪，保持只保留最新 N 条。
func (c *Client) ZRemRangeByRank(ctx context.Context, key string, start int64, stop int64) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.breaker.Execute(func() error {
		t := time.Now()
		err := c.rdb.ZRemRangeByRank(ctx, key, start, stop).Err()
		dur := time.Since(t).Seconds()
		if err != nil {
			observability.RedisOperationsTotal.WithLabelValues("zremrangebyrank", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("zremrangebyrank", "error").Observe(dur)
			return err
		}
		observability.RedisOperationsTotal.WithLabelValues("zremrangebyrank", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("zremrangebyrank", "success").Observe(dur)
		return nil
	})
}

// ZRangeWithScores 返回有序集合中指定排名范围的成员（带分数）。
// index=0 拿分数最小的（最老的数据），用于获取 watermark。
func (c *Client) ZRangeWithScores(ctx context.Context, key string, start int64, stop int64) ([]redis.Z, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	var vals []redis.Z
	err := c.breaker.Execute(func() error {
		t := time.Now()
		v, e := c.rdb.ZRangeWithScores(ctx, key, start, stop).Result()
		dur := time.Since(t).Seconds()
		if e != nil {
			observability.RedisOperationsTotal.WithLabelValues("zrangewithscores", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("zrangewithscores", "error").Observe(dur)
			return e
		}
		observability.RedisOperationsTotal.WithLabelValues("zrangewithscores", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("zrangewithscores", "success").Observe(dur)
		vals = v
		return nil
	})
	return vals, err
}

// ZRevRangeByScore 按分数从高到低返回成员，支持分页（offset+count）。
// 用于 listLatest 热数据查询：score = create_time 毫秒时间戳。
func (c *Client) ZRevRangeByScore(ctx context.Context, key string, max string, min string, offset int64, count int64) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	var vals []string
	err := c.breaker.Execute(func() error {
		t := time.Now()
		v, e := c.rdb.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
			Max:    max,
			Min:    min,
			Offset: offset,
			Count:  count,
		}).Result()
		dur := time.Since(t).Seconds()
		if e != nil {
			observability.RedisOperationsTotal.WithLabelValues("zrevrangebyscore", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("zrevrangebyscore", "error").Observe(dur)
			return e
		}
		observability.RedisOperationsTotal.WithLabelValues("zrevrangebyscore", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("zrevrangebyscore", "success").Observe(dur)
		vals = v
		return nil
	})
	return vals, err
}

// ZRevRange 按排名从高到低返回成员（不带分数）。
// 用于 listByPopularity 从合并后的热榜 ZSET 分页取数据。
func (c *Client) ZRevRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	var vals []string
	err := c.breaker.Execute(func() error {
		t := time.Now()
		v, e := c.rdb.ZRevRange(ctx, key, start, stop).Result()
		dur := time.Since(t).Seconds()
		if e != nil {
			observability.RedisOperationsTotal.WithLabelValues("zrevrange", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("zrevrange", "error").Observe(dur)
			return e
		}
		observability.RedisOperationsTotal.WithLabelValues("zrevrange", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("zrevrange", "success").Observe(dur)
		vals = v
		return nil
	})
	return vals, err
}

// ZUnionStoreWithWeights 将多个有序集合按指定权重合并到目标 key。
// weights 长度必须与 keys 一致，weights[i] 对应 keys[i] 的权重。
// 用于 listByPopularity 的时间衰减加权：越老的窗口权重越小。
func (c *Client) ZUnionStoreWithWeights(ctx context.Context, dst string, keys []string, weights []float64, aggregate string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.breaker.Execute(func() error {
		t := time.Now()
		err := c.rdb.ZUnionStore(ctx, dst, &redis.ZStore{
			Keys:      keys,
			Weights:   weights,
			Aggregate: aggregate,
		}).Err()
		dur := time.Since(t).Seconds()
		if err != nil {
			observability.RedisOperationsTotal.WithLabelValues("zunionstore", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("zunionstore", "error").Observe(dur)
			return err
		}
		observability.RedisOperationsTotal.WithLabelValues("zunionstore", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("zunionstore", "success").Observe(dur)
		return nil
	})
}

// Exists 检查 key 是否存在。
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, nil
	}
	var n int64
	err := c.breaker.Execute(func() error {
		t := time.Now()
		v, e := c.rdb.Exists(ctx, key).Result()
		dur := time.Since(t).Seconds()
		if e != nil {
			observability.RedisOperationsTotal.WithLabelValues("exists", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("exists", "error").Observe(dur)
			return e
		}
		observability.RedisOperationsTotal.WithLabelValues("exists", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("exists", "success").Observe(dur)
		n = v
		return nil
	})
	return n > 0, err
}

// ZCard 返回有序集合的成员数量，用于判断 inbox 是否为空。
func (c *Client) ZCard(ctx context.Context, key string) (int64, error) {
	if c == nil || c.rdb == nil {
		return 0, errors.New("redis client not initialized")
	}
	var count int64
	err := c.breaker.Execute(func() error {
		t := time.Now()
		v, e := c.rdb.ZCard(ctx, key).Result()
		dur := time.Since(t).Seconds()
		if e != nil {
			observability.RedisOperationsTotal.WithLabelValues("zcard", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("zcard", "error").Observe(dur)
			return e
		}
		observability.RedisOperationsTotal.WithLabelValues("zcard", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("zcard", "success").Observe(dur)
		count = v
		return nil
	})
	return count, err
}

// ZRevRangeWithScores 按排名从高到低返回成员（带分数），用于 inbox 查询时同时获取 videoID 和 createTime。
func (c *Client) ZRevRangeWithScores(ctx context.Context, key string, start, stop int64) ([]redis.Z, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	var vals []redis.Z
	err := c.breaker.Execute(func() error {
		t := time.Now()
		v, e := c.rdb.ZRevRangeWithScores(ctx, key, start, stop).Result()
		dur := time.Since(t).Seconds()
		if e != nil {
			observability.RedisOperationsTotal.WithLabelValues("zrevrangewithscores", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("zrevrangewithscores", "error").Observe(dur)
			return e
		}
		observability.RedisOperationsTotal.WithLabelValues("zrevrangewithscores", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("zrevrangewithscores", "success").Observe(dur)
		vals = v
		return nil
	})
	return vals, err
}

// GetRedisClient 返回底层 go-redis 客户端，供需要 Pipeline 等高级操作的场景使用。
// 外部不应长期持有此引用，仅在需要时临时获取。
func (c *Client) GetRedisClient() *redis.Client {
	if c == nil {
		return nil
	}
	return c.rdb
}
