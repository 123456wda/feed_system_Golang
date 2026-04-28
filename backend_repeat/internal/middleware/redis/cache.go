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
