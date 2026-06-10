package ratelimit

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"feedsystem_video_go/internal/middleware/jwt"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/observability"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

var memberSeq uint64

// slidingWindowScript 基于 Redis ZSET 的滑动窗口限流脚本。
//
// KEYS[1] = 限流 key
// ARGV[1] = 窗口长度（毫秒）
// ARGV[2] = 窗口内允许的最大请求数
// ARGV[3] = 当前时间戳（毫秒，由调用方传入避免 server-client 时钟漂移）
// ARGV[4] = 唯一 member 标识（避免同一毫秒内的多次请求互相覆盖）
//
// 算法：
//  1. ZREMRANGEBYSCORE: 移除窗口外的过期请求（score < now - window）
//  2. ZCARD: 统计当前窗口内的请求数
//  3. 未达上限 → ZADD 当前请求 + PEXPIRE 设置 TTL（防止冷 key 永久留在内存）
//  4. 已达上限 → 返回 1 拒绝
//
// 全部操作在 Lua 脚本里原子执行，不会出现"读到旧计数后再写"的竞态。
var slidingWindowScript = redis.NewScript(`
local key = KEYS[1]
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)

if count < limit then
    redis.call('ZADD', key, now, member)
    redis.call('PEXPIRE', key, window)
    return 0
else
    return 1
end
`)

// SlidingWindowLimit 创建滑动窗口限流中间件。
//
// 与 Limit（固定窗口）的区别：
//  1. 固定窗口存在"边界突刺"：在窗口边界两端各发起 maxRequests 次，瞬时 QPS 是限流值的 2 倍。
//     例如 1 分钟限流 100 次：第 59s 发 100 次 + 第 61s 发 100 次 = 2s 内 200 次。
//  2. 滑动窗口按"过去 window 时长内的请求总数"判断，无论何时切片都只允许 maxRequests 次。
//
// 代价：每次请求需要 ZREMRANGEBYSCORE + ZCARD + ZADD 三个 ZSET 操作，
// 比固定窗口的 INCR 多一些开销。在限流值不大的场景下可接受。
func SlidingWindowLimit(
	cache *rediscache.Client,
	keyPrefix string,
	maxRequests int64,
	window time.Duration,
	keyFunc KeyFunc,
) gin.HandlerFunc {
	return func(c *gin.Context) {
		if cache == nil || keyFunc == nil || maxRequests <= 0 || window <= 0 {
			c.Next()
			return
		}

		subject, ok := keyFunc(c)
		if !ok {
			c.Next()
			return
		}

		rdb := cache.GetRedisClient()
		if rdb == nil || cache.IsBreakerOpen() {
			c.Next()
			return
		}

		key := buildSlidingKey(keyPrefix, subject)
		t := time.Now()
		now := t.UnixMilli()
		// 纳秒时间戳 + 原子计数器保证并发唯一性
		seq := atomic.AddUint64(&memberSeq, 1)
		member := fmt.Sprintf("%d:%d", t.UnixNano(), seq)

		result, err := slidingWindowScript.Run(
			c.Request.Context(),
			rdb,
			[]string{key},
			window.Milliseconds(),
			maxRequests,
			now,
			member,
		).Int64()
		if err != nil {
			// Redis 异常，fail open
			c.Next()
			return
		}

		if result == 1 {
			observability.RateLimitRejections.WithLabelValues("sliding", normalizePrefix(keyPrefix)).Inc()
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "too many requests",
			})
			return
		}
		c.Next()
	}
}

// SlidingWindowLimitByIP 滑动窗口 + IP 维度限流的便捷构造器。
func SlidingWindowLimitByIP(cache *rediscache.Client, keyPrefix string, maxRequests int64, window time.Duration) gin.HandlerFunc {
	return SlidingWindowLimit(cache, keyPrefix, maxRequests, window, KeyByIP)
}

// SlidingWindowLimitByAccount 滑动窗口 + 用户维度限流的便捷构造器。
// 用于登录后的写操作限流（点赞/评论/关注）。
func SlidingWindowLimitByAccount(cache *rediscache.Client, keyPrefix string, maxRequests int64, window time.Duration) gin.HandlerFunc {
	return SlidingWindowLimit(cache, keyPrefix, maxRequests, window, KeyByAccountStrict)
}

// KeyByAccountStrict 与 KeyByAccount 相同，但用 jwt.GetAccountID 取值。
// 抽出独立函数避免和 ratelimit.go 的 KeyByAccount 重复绑定。
func KeyByAccountStrict(c *gin.Context) (string, bool) {
	accountID, err := jwt.GetAccountID(c)
	if err != nil || accountID == 0 {
		return "", false
	}
	return fmt.Sprintf("%d", accountID), true
}

func buildSlidingKey(keyPrefix, subject string) string {
	keyPrefix = strings.TrimSpace(keyPrefix)
	if keyPrefix == "" {
		keyPrefix = "default"
	}
	return fmt.Sprintf("feedsystem:ratelimit:sw:%s:%s", keyPrefix, strings.TrimSpace(subject))
}

func normalizePrefix(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return "default"
	}
	return p
}
