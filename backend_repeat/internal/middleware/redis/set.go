package redis

import (
	"context"
	"errors"
	"time"

	"feedsystem_video_go/internal/observability"
)

// SAdd 向集合添加一个或多个成员，用于维护用户关注的大V列表。
// 如果成员已存在则忽略（幂等操作）。
func (c *Client) SAdd(ctx context.Context, key string, members ...any) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	return c.breaker.Execute(func() error {
		start := time.Now()
		err := c.rdb.SAdd(ctx, key, members...).Err()
		dur := time.Since(start).Seconds()
		if err != nil {
			observability.RedisOperationsTotal.WithLabelValues("sadd", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("sadd", "error").Observe(dur)
			return err
		}
		observability.RedisOperationsTotal.WithLabelValues("sadd", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("sadd", "success").Observe(dur)
		return nil
	})
}

// SRem 从集合移除一个或多个成员，用于取关时清理大V关注列表。
// 如果成员不存在则忽略（幂等操作）。
func (c *Client) SRem(ctx context.Context, key string, members ...any) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client not initialized")
	}
	return c.breaker.Execute(func() error {
		start := time.Now()
		err := c.rdb.SRem(ctx, key, members...).Err()
		dur := time.Since(start).Seconds()
		if err != nil {
			observability.RedisOperationsTotal.WithLabelValues("srem", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("srem", "error").Observe(dur)
			return err
		}
		observability.RedisOperationsTotal.WithLabelValues("srem", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("srem", "success").Observe(dur)
		return nil
	})
}

// SMembers 返回集合中的所有成员，用于获取用户关注的大V列表。
func (c *Client) SMembers(ctx context.Context, key string) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	var vals []string
	err := c.breaker.Execute(func() error {
		start := time.Now()
		v, e := c.rdb.SMembers(ctx, key).Result()
		dur := time.Since(start).Seconds()
		if e != nil {
			observability.RedisOperationsTotal.WithLabelValues("smembers", "error").Inc()
			observability.RedisOperationDuration.WithLabelValues("smembers", "error").Observe(dur)
			return e
		}
		observability.RedisOperationsTotal.WithLabelValues("smembers", "success").Inc()
		observability.RedisOperationDuration.WithLabelValues("smembers", "success").Observe(dur)
		vals = v
		return nil
	})
	return vals, err
}
