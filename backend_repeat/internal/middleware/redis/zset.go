package redis

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

// MGet 批量获取多个 key 的值，用于 feed 流批量查询视频缓存。
func (c *Client) MGet(ctx context.Context, keys ...string) ([]interface{}, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	return c.rdb.MGet(ctx, keys...).Result()
}

// ZAdd 向有序集合添加成员。
func (c *Client) ZAdd(ctx context.Context, key string, members ...redis.Z) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZAdd(ctx, key, members...).Err()
}

// ZRemRangeByRank 移除有序集合中指定排名范围的成员。
// 用于 timeline ZSET 裁剪，保持只保留最新 N 条。
func (c *Client) ZRemRangeByRank(ctx context.Context, key string, start int64, stop int64) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZRemRangeByRank(ctx, key, start, stop).Err()
}

// ZRangeWithScores 返回有序集合中指定排名范围的成员（带分数）。
// index=0 拿分数最小的（最老的数据），用于获取 watermark。
func (c *Client) ZRangeWithScores(ctx context.Context, key string, start int64, stop int64) ([]redis.Z, error) {
	if c == nil || c.rdb == nil {
		return nil, errors.New("redis client not initialized")
	}
	return c.rdb.ZRangeWithScores(ctx, key, start, stop).Result()
}

// ZRevRangeByScore 按分数从高到低返回成员，支持分页（offset+count）。
// 用于 listLatest 热数据查询：score = create_time 毫秒时间戳。
func (c *Client) ZRevRangeByScore(ctx context.Context, key string, max string, min string, offset int64, count int64) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	return c.rdb.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
		Max:    max,
		Min:    min,
		Offset: offset,
		Count:  count,
	}).Result()
}

// ZRevRange 按排名从高到低返回成员（不带分数）。
// 用于 listByPopularity 从合并后的热榜 ZSET 分页取数据。
func (c *Client) ZRevRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	if c == nil || c.rdb == nil {
		return nil, nil
	}
	return c.rdb.ZRevRange(ctx, key, start, stop).Result()
}

// ZUnionStore 将多个有序集合合并（求和）到目标 key。
// 用于 listByPopularity：合并过去 60 个 1 分钟窗口的热度 ZSET。
func (c *Client) ZUnionStore(ctx context.Context, dst string, keys []string, aggregate string) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.ZUnionStore(ctx, dst, &redis.ZStore{
		Keys:      keys,
		Aggregate: aggregate,
	}).Err()
}

// Exists 检查 key 是否存在。
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, nil
	}
	n, err := c.rdb.Exists(ctx, key).Result()
	return n > 0, err
}
