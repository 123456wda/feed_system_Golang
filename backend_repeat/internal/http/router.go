package http

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/middleware/rabbitmq"
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
