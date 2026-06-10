package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"feedsystem_video_go/internal/config"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb     *redis.Client
	breaker *Breaker
}

// 创建Redis客户端连接服务器
func NewFromEnv(cfg *config.RedisConfig) (*Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:        cfg.Host + ":" + strconv.Itoa(cfg.Port),
		Password:    cfg.Password,
		DB:          cfg.DB,
		DialTimeout: 5 * time.Second,
	})

	return &Client{
		rdb:     client,
		breaker: NewBreaker(DefaultBreakerConfig()),
	}, nil
}

// NewFromAddr 直接基于地址构造客户端，主要用于测试（如 miniredis）。
func NewFromAddr(addr string) *Client {
	return &Client{
		rdb:     redis.NewClient(&redis.Options{Addr: addr}),
		breaker: NewBreaker(DefaultBreakerConfig()),
	}
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

// redis实现限流
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

func IsMiss(err error) bool {
	return errors.Is(err, redis.Nil)
}

// 起到一个类似uuid的作用,生成一个随机字符串
func randToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (c *Client) Lock(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	if c == nil || c.rdb == nil {
		return "", false, nil
	}
	token, err := randToken(16)
	if err != nil {
		return "", false, err
	}
	ok, err := c.rdb.SetNX(ctx, key, token, ttl).Result()
	if err != nil {
		return "", false, err
	}
	return token, ok, nil
}

var unlockScript = redis.NewScript(
	`
	local token = redis.call('GET',KEYS[1])
	if token==ARGV[1] then
		return redis.call('DEL',KEYS[1])
	else
		return 0
	end	
	`,
)

func (c *Client) UnLock(ctx context.Context, key string, token string) error {
	if c == nil || c.rdb == nil {
		return errors.New("redis client is nil")
	}
	_, err := unlockScript.Run(ctx, c.rdb, []string{key}, token).Result()

	return err
}

//--------------------------------以上是手动实现分布式锁---------------------------------------------------------//
