package ratelimit

import (
	"fmt"
	"strings"
	"time"

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
		//下面对redis缓存操作
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
