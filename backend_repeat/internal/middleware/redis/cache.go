package redis

import (
	"context"
	"time"
)

func (c *Client) SetBytes(ctx context.Context, key string, value []byte, time time.Duration) error {
	return c.rdb.Set(ctx, key, value, time).Err()
}

func (c *Client) Del(ctx context.Context, key string) error {
	return c.rdb.Del(ctx, key).Err()
}

func (c *Client) GetBytes(ctx context.Context, key string) ([]byte, error) {
	val, err := c.rdb.Get(ctx, key).Result()
	return []byte(val), err
}

// ZincrBy 给有序集合成员增加分数，用于视频热度排行（每分钟窗口）。
// 返回操作后成员的最新分数。
func (c *Client) ZincrBy(ctx context.Context, key string, member string, increment float64) error {
	return c.rdb.ZIncrBy(ctx, key, increment, member).Err()
}

// Expire 给 key 设置过期时间，用于控制热度窗口的生命周期。
func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return c.rdb.Expire(ctx, key, ttl).Err()
}
