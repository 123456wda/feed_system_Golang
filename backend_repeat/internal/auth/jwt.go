package auth

import (
	"errors"
	"log"
	"os"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

/*
(1)创建一个Claims对象
(2)注册一个token机
(3)对该token机签发


(1)对该tokenstring解析
(2)获得解析后的Claims对象
(3)返回该对象
*/

const (
	defaultJWTSecret = "feedsystem_secret"
)

var warnOnce sync.Once

func jwtSecret() []byte {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		warnOnce.Do(func() {
			log.Printf("[WARNING] JWT_SECRET env var not set, using default secret. "+
				"This is INSECURE and should only be used for local development. "+
				"Set JWT_SECRET environment variable for production.")
		})
		secret = defaultJWTSecret
	}

	return []byte(secret)
}

type Claims struct {
	AccountID uint   `json:"account_id"`
	Username  string `json:"username"`
	jwt.RegisteredClaims
}

func GenerateToken(accountID uint, Username string) (string, error) {
	now := time.Now()
	claims := Claims{
		AccountID: accountID,
		Username:  Username,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(24 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(jwtSecret())
}

func ParseToken(tokenstring string) (*Claims, error) {
	claim := &Claims{}
	token, err := jwt.ParseWithClaims(tokenstring, claim, func(token *jwt.Token) (interface{}, error) {
		if token.Method == nil || token.Method.Alg() != jwt.SigningMethodHS256.Alg() {
			return nil, errors.New("unexpected signing method")
		}
		return jwtSecret(), nil
	},
	)
	if err != nil {
		return nil, err
	}
	if !token.Valid {
		return nil, jwt.ErrTokenInvalidClaims
	}
	return claim, nil
}
