package ratelimit

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"feedsystem_video_go/internal/middleware/jwt"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"github.com/gin-gonic/gin"
)

type KeyFunc func(c *gin.Context) (string, bool)

/*
基本思路,对于account/login(例子)这个请求,根据ip地址和对应请求的前缀组成键,在redis里面自增后取值判断是否在指定时间超过最大限制
->未超过限制 Next()
->超过限制  Abort()
*/
func Limit(
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
		// 根据IP获取键值主体
		subject, ok := keyFunc(c)
		if !ok {
			c.Next()
			return
		}
		key := buildKey(keyPrefix, subject)
		fmt.Println(key)
		// 下面对redis缓存操作
		count, err := cache.IncrementWithExpire(c.Request.Context(), key, window)
		if err != nil {
			c.Next()
			return
		}
		if count > maxRequests {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": "too many requests",
			})
		}
		c.Next()
	}
}

func buildKey(keyPrefix, subject string) string {
	keyPrefix = strings.TrimSpace(keyPrefix)
	if keyPrefix == "" {
		keyPrefix = "default"
	}
	return fmt.Sprintf("feedsystem:ratelimit:%s:%s", keyPrefix, strings.TrimSpace(subject))
}

func KeyByIP(c *gin.Context) (string, bool) {
	ip := strings.TrimSpace(c.ClientIP())
	if ip == "" {
		return "", false
	}
	return ip, true
}

// KeyByAccount 从 JWT 中提取用户 ID 作为限流 key。
// 用于登录后的写操作限流（如点赞、评论），按用户维度限制频率。
func KeyByAccount(c *gin.Context) (string, bool) {
	accountID, err := jwt.GetAccountID(c)
	if err != nil || accountID == 0 {
		return "", false
	}
	return strconv.FormatUint(uint64(accountID), 10), true
}
