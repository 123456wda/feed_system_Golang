package redis

import (
	"context"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

// TestIncrementWithExpireSetsTTLWithoutExtendingWindow 验证限流 Lua 脚本的核心语义：
// 首次 INCR 时设置过期时间，后续 INCR 只累加计数、不续期。
// 这保证了限流窗口是固定的，不会因为持续请求而被无限延长。
func TestIncrementWithExpireSetsTTLWithoutExtendingWindow(t *testing.T) {
	// 启动 miniredis（纯内存 Redis 替代品，无需真实 Redis 实例）
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	client := &Client{
		rdb: goredis.NewClient(&goredis.Options{Addr: mr.Addr()}),
	}
	defer client.Close()

	ctx := context.Background()
	key := "feedsystem:ratelimit:test"
	expire := 30 * time.Second

	// 第一次 INCR：计数应为 1，且 TTL 被设置为 expire
	count, err := client.IncrementWithExpire(ctx, key, expire)
	if err != nil {
		t.Fatalf("first increment: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}

	firstTTL := mr.TTL(key)
	if firstTTL <= 0 || firstTTL > expire {
		t.Fatalf("expected ttl in (0, %s], got %s", expire, firstTTL)
	}

	// 模拟时间流逝 5 秒，TTL 从 30s 降到约 25s
	mr.FastForward(5 * time.Second)
	ttlBeforeSecond := mr.TTL(key)

	// 第二次 INCR：计数应为 2，关键验证点——TTL 不应被重置
	count, err = client.IncrementWithExpire(ctx, key, expire)
	if err != nil {
		t.Fatalf("second increment: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected count 2, got %d", count)
	}

	// 如果脚本写错（每次都 PEXPIRE），TTL 会被重置回 30s，窗口被延长
	ttlAfterSecond := mr.TTL(key)
	if ttlAfterSecond != ttlBeforeSecond {
		t.Fatalf("expected ttl to stay at %s, got %s", ttlBeforeSecond, ttlAfterSecond)
	}
}
