package jwt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/auth"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"github.com/gin-gonic/gin"
)

func JWTAuth(accountRepo *account.AccountRepository, cache *rediscache.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 获取认证头
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing authorization header"})
			return
		}

		// 分割认证头
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing authorization header"})
			return
		}

		tokenString := parts[1]
		// 验证
		claims, err := auth.ParseToken(tokenString)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired token"})
			return
		}
		check(c, claims, tokenString, accountRepo, cache)
	}
}

func check(c *gin.Context, claims *auth.Claims, tokenString string, accountRepo *account.AccountRepository, cache *rediscache.Client) {
	// 现在缓存里面查找
	key := fmt.Sprintf("account:%d", claims.AccountID)

	if cache != nil {
		cacheCtx, cancel := context.WithTimeout(context.Background(), time.Microsecond*50)
		defer cancel()
		trueToken, err := cache.GetBytes(cacheCtx, key)
		if err != nil {
			log.Printf("warning about cache(used for check token): %v", err)
		} else {
			if string(trueToken) != tokenString {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "token has been revoked"})
				return
			}
			c.Set("accountID", claims.AccountID)
			c.Set("username", claims.Username)
			c.Next()
			return
		}

	}
	// redis出现异常,MySql兜底
	accountInfo, err := accountRepo.FindByID(c.Request.Context(), claims.AccountID)
	if err != nil || accountInfo.Token == "" || accountInfo.Token != tokenString {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "token has been revoked"})
		return
	}

	// 尝试回源redias
	if cache != nil {
		cacheCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		if err := cache.SetBytes(cacheCtx, key, []byte(accountInfo.Token), time.Hour*24); err != nil {
			log.Printf("warning about cache(used for check token): %v", err)
		}
	}

	c.Set("accountID", accountInfo.ID)
	c.Set("username", accountInfo.Username)
	c.Next()
	return
}

func GetAccountID(c *gin.Context) (uint, error) {
	uidValue, exists := c.Get("accountID")
	if !exists {
		return 0, errors.New("accountID not found")
	}
	accountID, ok := uidValue.(uint)
	if !ok {
		return 0, errors.New("accountID has invalid type")
	}
	return accountID, nil
}

func GetUsername(c *gin.Context) (string, error) {
	val, exists := c.Get("username")
	if !exists {
		return "", errors.New("username not found")
	}
	username, ok := val.(string)
	if !ok {
		return "", errors.New("username has invalid type")
	}
	return username, nil
}
