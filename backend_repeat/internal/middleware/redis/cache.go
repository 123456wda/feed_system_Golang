package redis

import (
	"context"
	"time"

	"feedsystem_video_go/internal/observability"
)

func (c *Client) SetBytes(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.breaker.Execute(func() error {
		start := time.Now()
		err := c.rdb.Set(ctx, key, value, ttl).Err()
		dur := time.Since(start).Seconds()
		if err != nil {
			observability.RedisOperationsTotal.WithLabelValues("set", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("set", "error").Observe(dur)
			return err
		}
		observability.RedisOperationsTotal.WithLabelValues("set", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("set", "success").Observe(dur)
		return nil
	})
}

func (c *Client) Del(ctx context.Context, key string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.breaker.Execute(func() error {
		start := time.Now()
		err := c.rdb.Del(ctx, key).Err()
		dur := time.Since(start).Seconds()
		if err != nil {
			observability.RedisOperationsTotal.WithLabelValues("del", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("del", "error").Observe(dur)
			return err
		}
		observability.RedisOperationsTotal.WithLabelValues("del", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("del", "success").Observe(dur)
		return nil
	})
}

func (c *Client) GetBytes(ctx context.Context, key string) ([]byte, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	var val string
	err := c.breaker.Execute(func() error {
		start := time.Now()
		v, e := c.rdb.Get(ctx, key).Result()
		dur := time.Since(start).Seconds()
		if e != nil {
			if IsMiss(e) {
				// redis.Nil 是缓存未命中，属于正常业务结果
				observability.RedisOperationsTotal.WithLabelValues("get", "miss").Inc()
				observability.RedisOperationDuration.WithLabelValues("get", "miss").Observe(dur)
			} else {
				observability.RedisOperationsTotal.WithLabelValues("get", "error").Inc()
				observability.RedisOperationDuration.WithLabelValues("get", "error").Observe(dur)
			}
			return e
		}
		observability.RedisOperationsTotal.WithLabelValues("get", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("get", "success").Observe(dur)
		val = v
		return nil
	})
	return []byte(val), err
}

// ZincrBy 给有序集合成员增加分数，用于视频热度排行（每分钟窗口）。
func (c *Client) ZincrBy(ctx context.Context, key string, member string, increment float64) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.breaker.Execute(func() error {
		start := time.Now()
		err := c.rdb.ZIncrBy(ctx, key, increment, member).Err()
		dur := time.Since(start).Seconds()
		if err != nil {
			observability.RedisOperationsTotal.WithLabelValues("zincrby", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("zincrby", "error").Observe(dur)
			return err
		}
		observability.RedisOperationsTotal.WithLabelValues("zincrby", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("zincrby", "success").Observe(dur)
		return nil
	})
}

// Expire 给 key 设置过期时间，用于控制热度窗口的生命周期。
func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.breaker.Execute(func() error {
		start := time.Now()
		err := c.rdb.Expire(ctx, key, ttl).Err()
		dur := time.Since(start).Seconds()
		if err != nil {
			observability.RedisOperationsTotal.WithLabelValues("expire", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("expire", "error").Observe(dur)
			return err
		}
		observability.RedisOperationsTotal.WithLabelValues("expire", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("expire", "success").Observe(dur)
		return nil
	})
}
