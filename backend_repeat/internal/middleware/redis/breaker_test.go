package redis

import (
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestBreakerTripsOnConsecutiveFailures 验证连续失败到达阈值后熔断器拒绝后续请求。
func TestBreakerTripsOnConsecutiveFailures(t *testing.T) {
	cfg := BreakerConfig{
		Name:                "test",
		MaxRequests:         1,
		Interval:            time.Minute,
		Timeout:             100 * time.Millisecond,
		ConsecutiveFailures: 3,
	}
	b := NewBreaker(cfg)

	if b.State() != "closed" {
		t.Fatalf("expected initial state closed, got %s", b.State())
	}

	bizErr := errors.New("boom")
	for i := 0; i < 3; i++ {
		err := b.Execute(func() error { return bizErr })
		if !errors.Is(err, bizErr) {
			t.Fatalf("iter %d: expected biz error, got %v", i, err)
		}
	}

	// 触发熔断后立即拒绝
	if b.State() != "open" {
		t.Fatalf("expected open after %d failures, got %s", cfg.ConsecutiveFailures, b.State())
	}
	err := b.Execute(func() error {
		t.Fatal("fn should not run when breaker is open")
		return nil
	})
	if !errors.Is(err, ErrBreakerOpen) {
		t.Fatalf("expected ErrBreakerOpen, got %v", err)
	}
}

// TestBreakerRecoversAfterTimeout 验证 Timeout 后切到 HalfOpen，成功后回到 Closed。
func TestBreakerRecoversAfterTimeout(t *testing.T) {
	cfg := BreakerConfig{
		Name:                "test",
		MaxRequests:         1,
		Interval:            time.Minute,
		Timeout:             50 * time.Millisecond,
		ConsecutiveFailures: 1,
	}
	b := NewBreaker(cfg)

	bizErr := errors.New("down")
	_ = b.Execute(func() error { return bizErr })
	if b.State() != "open" {
		t.Fatalf("expected open, got %s", b.State())
	}

	// 等待 Timeout，应进入 HalfOpen 探测
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) && b.State() == "open" {
		time.Sleep(5 * time.Millisecond)
	}
	if b.State() == "open" {
		t.Fatal("breaker did not transition to half_open within deadline")
	}

	// 第一个探测请求成功 → 回到 Closed
	if err := b.Execute(func() error { return nil }); err != nil {
		t.Fatalf("expected probe success, got %v", err)
	}
	if b.State() != "closed" {
		t.Fatalf("expected closed after successful probe, got %s", b.State())
	}
}

// TestBreakerHalfOpenFailsBackToOpen 验证 HalfOpen 探测失败立即回到 Open。
func TestBreakerHalfOpenFailsBackToOpen(t *testing.T) {
	cfg := BreakerConfig{
		Name:                "test",
		MaxRequests:         1,
		Interval:            time.Minute,
		Timeout:             50 * time.Millisecond,
		ConsecutiveFailures: 1,
	}
	b := NewBreaker(cfg)

	bizErr := errors.New("err")
	_ = b.Execute(func() error { return bizErr })

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) && b.State() == "open" {
		time.Sleep(5 * time.Millisecond)
	}

	// HalfOpen 状态下探测失败
	_ = b.Execute(func() error { return bizErr })

	if b.State() != "open" {
		t.Fatalf("expected open after failed probe, got %s", b.State())
	}
}

// TestBreakerCacheMissNotCountedAsFailure 验证 redis.Nil（缓存未命中）不会触发熔断。
// 这是关键设计：缓存查不到只意味着回源 MySQL，不是 Redis 故障。
func TestBreakerCacheMissNotCountedAsFailure(t *testing.T) {
	cfg := BreakerConfig{
		Name:                "test",
		MaxRequests:         1,
		Interval:            time.Minute,
		Timeout:             time.Second,
		ConsecutiveFailures: 2,
	}
	b := NewBreaker(cfg)

	// 模拟 10 次缓存未命中
	for i := 0; i < 10; i++ {
		err := b.Execute(func() error { return redis.Nil })
		if !errors.Is(err, redis.Nil) {
			t.Fatalf("expected redis.Nil to passthrough, got %v", err)
		}
	}

	if b.State() != "closed" {
		t.Fatalf("cache misses must not trip breaker, state=%s", b.State())
	}
}

// TestBreakerNilSafe 验证 nil breaker 直接执行 fn，不 panic。
func TestBreakerNilSafe(t *testing.T) {
	var b *Breaker

	called := false
	err := b.Execute(func() error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if !called {
		t.Fatal("fn must execute when breaker is nil")
	}
}

// TestBreakerSuccessThroughClosed 验证 Closed 状态下成功不影响状态。
func TestBreakerSuccessThroughClosed(t *testing.T) {
	b := NewBreaker(DefaultBreakerConfig())

	for i := 0; i < 100; i++ {
		if err := b.Execute(func() error { return nil }); err != nil {
			t.Fatalf("expected success, got %v", err)
		}
	}
	if b.State() != "closed" {
		t.Fatalf("expected closed, got %s", b.State())
	}
}
