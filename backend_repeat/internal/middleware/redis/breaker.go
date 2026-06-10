package redis

import (
	"errors"
	"time"

	"feedsystem_video_go/internal/observability"

	"github.com/sony/gobreaker/v2"
)

// ErrBreakerOpen 当熔断器处于 Open 或 HalfOpen 拒绝额外请求时返回。
// 与 gobreaker.ErrOpenState / ErrTooManyRequests 等价，统一对外暴露。
var ErrBreakerOpen = errors.New("circuit breaker is open")

// Breaker 是 sony/gobreaker 的薄封装，提供 Prometheus 指标上报与统一的错误语义。
//
// gobreaker 的状态机：
//   - StateClosed   : 正常放行，统计失败率
//   - StateOpen     : 直接拒绝（返回 ErrOpenState）
//   - StateHalfOpen : 放行少量探测请求，达到阈值后回到 Closed，否则回到 Open
//
// 项目使用场景：包裹 Redis 调用，避免 Redis 抖动时大量请求级联超时。
type Breaker struct {
	cb *gobreaker.CircuitBreaker[any]
}

// BreakerConfig 熔断器配置。
type BreakerConfig struct {
	// Name 熔断器名称，会出现在状态切换日志中。
	Name string
	// MaxRequests HalfOpen 状态下允许放行的探测请求数。达到该数量后进入下一次评估。
	MaxRequests uint32
	// Interval Closed 状态下的统计窗口长度。窗口结束后失败计数清零，避免"间歇性失败永远累加"。
	Interval time.Duration
	// Timeout Open 状态持续多久后切到 HalfOpen 探测恢复。
	Timeout time.Duration
	// ConsecutiveFailures 连续失败多少次后切到 Open。
	ConsecutiveFailures uint32
}

// DefaultBreakerConfig 适合 Redis 调用的默认配置：
//   - 连续 5 次失败 → 熔断
//   - 熔断后 10s 进入 HalfOpen 探测
//   - HalfOpen 放行 1 个探测请求
//   - Closed 状态 60s 滚动窗口
func DefaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		Name:                "redis",
		MaxRequests:         1,
		Interval:            60 * time.Second,
		Timeout:             10 * time.Second,
		ConsecutiveFailures: 5,
	}
}

// NewBreaker 创建熔断器实例，状态切换会上报到 Prometheus。
func NewBreaker(cfg BreakerConfig) *Breaker {
	if cfg.Name == "" {
		cfg.Name = "redis"
	}
	if cfg.MaxRequests == 0 {
		cfg.MaxRequests = 1
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 60 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.ConsecutiveFailures == 0 {
		cfg.ConsecutiveFailures = 5
	}

	settings := gobreaker.Settings{
		Name:        cfg.Name,
		MaxRequests: cfg.MaxRequests,
		Interval:    cfg.Interval,
		Timeout:     cfg.Timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= cfg.ConsecutiveFailures
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			observability.CircuitBreakerStateChanges.WithLabelValues(stateLabel(to)).Inc()
		},
	}
	return &Breaker{cb: gobreaker.NewCircuitBreaker[any](settings)}
}

// IsBreakerOpen 返回熔断器是否处于 Open 状态。
// 供 Pipeline 等无法通过 Execute 包裹的场景在执行前快速判断，避免在 Redis 已知不可用时阻塞等待超时。
func (c *Client) IsBreakerOpen() bool {
	if c == nil || c.breaker == nil {
		return false
	}
	s := c.breaker.State()
	return s == "open" || s == "half_open"
}

// State 返回当前状态字符串（"closed" / "open" / "half_open"），供监控/测试使用。
func (b *Breaker) State() string {
	if b == nil || b.cb == nil {
		return "closed"
	}
	return stateLabel(b.cb.State())
}

// Execute 在熔断器保护下执行 fn。
// 关键设计：Redis 缓存未命中（redis.Nil）不计入熔断失败，但原始错误会透传给调用方，
// 因为调用方需要根据 redis.Nil 判断是否回源 MySQL。
func (b *Breaker) Execute(fn func() error) error {
	if b == nil || b.cb == nil {
		return fn()
	}

	// 用闭包变量捕获 fn 的原始错误，
	// 然后向 gobreaker 报告"过滤后的错误"（miss 时报 nil，其他原样上报）。
	var origErr error
	_, cbErr := b.cb.Execute(func() (any, error) {
		origErr = fn()
		if origErr != nil && IsMiss(origErr) {
			// 缓存未命中是正常业务结果，不计入熔断失败统计
			return nil, nil
		}
		return nil, origErr
	})

	if cbErr != nil {
		// gobreaker 拒绝时返回 ErrOpenState 或 ErrTooManyRequests
		if errors.Is(cbErr, gobreaker.ErrOpenState) || errors.Is(cbErr, gobreaker.ErrTooManyRequests) {
			observability.CircuitBreakerRejections.Inc()
			return ErrBreakerOpen
		}
		// 业务错误从 fn 透传（cbErr 此时等于 origErr）
		return cbErr
	}

	// gobreaker 没报错：要么 fn 真成功（origErr == nil），要么是 cache miss（origErr == redis.Nil）
	return origErr
}

func stateLabel(s gobreaker.State) string {
	switch s {
	case gobreaker.StateClosed:
		return "closed"
	case gobreaker.StateOpen:
		return "open"
	case gobreaker.StateHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}
