package redis

import (
	"context"
	"strconv"
	"time"

	"feedsystem_video_go/internal/config"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb *redis.Client
}

// 创建Redis客户端连接服务器
func NewFromEnv(cfg *config.RedisConfig) (*Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Host + ":" + strconv.Itoa(cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	return &Client{rdb: client}, nil
}

// 关闭redis连接

func (c *Client) Close() error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Close()
}

// 测试连接是否成功建立
func (c *Client) Ping(ctx context.Context) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.Ping(ctx).Err()
}

var incrementWithExpireScript = redis.NewScript(
	`
	local count = redis.call('INCR',KEYS[1])
	if count==1 then
		redis.call('PEXPIRE',KEYS[1],ARGV[1])
	end 
	return count 
	`,
)

func (c *Client) IncrementWithExpire(ctx context.Context, key string, expireTime time.Duration) (int64, error) {
	if c == nil || c.rdb == nil {
		return 0, nil
	}
	return incrementWithExpireScript.Run(
		ctx,
		c.rdb,
		[]string{key},
		expireTime.Milliseconds(),
	).Int64()
}
