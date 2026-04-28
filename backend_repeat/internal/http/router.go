package http

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/middleware/jwt"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/middleware/ratelimit"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetRouter(db *gorm.DB, cache *rediscache.Client, rbq *rabbitmq.RabbitMQ) *gin.Engine {
	r := gin.Default()
	// 不信任任何代理的转发ip,直接取到实际的tcp连接ip
	if err := r.SetTrustedProxies(nil); err != nil {
		log.Printf("SetTrustedProxies failed: %v", err)
	}
	// 把URL路径/static映射为对应路径,目的是方便加载本地视频资源
	r.Static("/static", "./.run/uploads")

	// 创建account相关限流器
	loginLimiter := ratelimit.Limit(cache, "account_login", 10, time.Minute, ratelimit.KeyByIP)
	registerLimiter := ratelimit.Limit(cache, "account_register", 10, time.Minute, ratelimit.KeyByIP)

	// 创建account相关业务路由组以及路由
	/*
		这里涉及一个层层封装
		最底层封装account仓库,操作数据层
		外包一层service,负责处理业务逻辑
		最外层包一层handler,负责处理响应请求
	*/
	accountRepository := account.NewAccountRepository(db)
	accountService := account.NewAccountService(accountRepository, cache)
	accountHandler := account.NewAccountHandler(accountService)
	accountGroup := r.Group("/account")
	{
		accountGroup.POST("/register", registerLimiter, accountHandler.CreateAccount)
		accountGroup.POST("/login", loginLimiter, accountHandler.Login)
		accountGroup.POST("/changePassword", accountHandler.ChangePassword)
		accountGroup.POST("/findByID", accountHandler.FindByID)
		accountGroup.POST("/findByUsername", accountHandler.FindByUsername)
	}
	protectedAccountGroup := accountGroup.Group("")
	protectedAccountGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedAccountGroup.POST("/logout", accountHandler.Logout)
		protectedAccountGroup.POST("/rename", accountHandler.Rename)
	}
	return r
}

func StartServer(srv *http.Server, cfg *config.ServerConfig) {
	log.Printf("api server started at :%d", cfg.Port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen server failed: %v", err)
	}
}

func GracefulShutdown(srv *http.Server) {
	// 监听关闭型号
	stop := make(chan os.Signal, 1)

	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	// 直到接收到信号才会继续执行
	<-stop
	log.Printf("api server is shutting down...")

	// 设置超时时间,走完正在进行的请求
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("api server shutdown failed: %v", err)
	}
	log.Printf("server exited gracefully")
}
