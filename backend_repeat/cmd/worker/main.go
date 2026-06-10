package main

// Worker process for consuming business events from RabbitMQ.
/*
1. API service
   - receive requests
   - validate auth and params
   - return quickly
   - publish events to RabbitMQ

2. Worker service
   - consume events from RabbitMQ
   - handle async work
   - persist data, update cache, and update counters
*/

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/db"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/observability"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	"feedsystem_video_go/internal/worker"
)

const (
	// Social MQ topology.
	socialExchange   = "social.events"
	socialQueue      = "social.events"
	socialBindingKey = "social.*"

	// Like MQ topology.
	likeExchange   = "like.events"
	likeQueue      = "like.events"
	likeBindingKey = "like.*"

	// Comment MQ topology.
	commentExchange   = "comment.events"
	commentQueue      = "comment.events"
	commentBindingKey = "comment.*"

	// Popularity MQ topology.
	popularityExchange   = "video.popularity.events"
	popularityQueue      = "video.popularity.events"
	popularityBindingKey = "video.popularity.*"

	// Fanout MQ topology（复用 timeline exchange，独立队列和 routing key）。
	fanoutExchange   = "video.timeline.events"
	fanoutQueue      = "video.timeline.fanout.queue"
	fanoutBindingKey = "video.timeline.fanout"

	prefetchCount  = 50
	prefetchSize   = 0
	prefetchGlobal = false
)

func main() {
	// Read config.
	configPath := os.Getenv("CONFIG_PATH")
	configPath_docker := flag.String("config", "", "配置文件路径")
	flag.Parse()
	configPath = *configPath_docker
	if configPath == "" {
		configPath = "configs/config.yaml"
	}
	log.Printf("Loading config from %s", configPath)
	cfg, usedDefault, err := config.LoadLocalDev(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	if usedDefault {
		log.Printf("Config File %s not found, using default local config", configPath)
	} else {
		log.Printf("Config loaded from file: %s", configPath)
	}
	// Connect database.
	sqlDB, err := db.NewDB(cfg.Database)
	if err != nil {
		log.Fatalf("Failed to connect database: %v", err)
	}
	defer db.CloseDB(sqlDB)

	// Connect Redis for popularity updates.
	cache, err := rediscache.NewFromEnv(&cfg.Redis)
	if err != nil {
		log.Printf("Redis config error (popularity worker disabled): %v", err)
		cache = nil
	} else {
		pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := cache.Ping(pingCtx); err != nil {
			log.Printf("Redis not available (popularity worker disabled): %v", err)
			_ = cache.Close()
			cache = nil
		} else {
			defer cache.Close()
			log.Printf("Redis connected (popularity worker enabled)")
		}
	}

	// Connect RabbitMQ.
	rbq, err := rabbitmq.NewRabbitMQ(&cfg.RabbitMQ)
	if err != nil {
		log.Fatalf("Failed to initialze RabbitMQ: %v", err)
	}
	defer rbq.Close()

	// Declare MQ topology.
	if err := declareSocialTopology(rbq); err != nil {
		log.Fatalf("Failed to declare social topology: %v", err)
	}
	if err := declareLikeTopology(rbq); err != nil {
		log.Fatalf("Failed to declare like topology: %v", err)
	}
	if err := declareCommentTopology(rbq); err != nil {
		log.Fatalf("Failed to declare comment topology: %v", err)
	}
	if cache != nil {
		if err := declarePopularityTopology(rbq); err != nil {
			log.Fatalf("Failed to declare popularity topology: %v", err)
		}
	}

	// Configure consumer prefetch.
	if err := rbq.Ch.Qos(prefetchCount, prefetchSize, prefetchGlobal); err != nil {
		log.Fatalf("Failed to set Qos: %v", err)
	}

	// Declare fanout topology（推拉结合的推路径 MQ）。
	if cache != nil {
		if err := declareFanoutTopology(rbq); err != nil {
			log.Fatalf("Failed to declare fanout topology: %v", err)
		}
	}

	// Create repositories and workers.
	socialRepo := social.NewSocialRepository(sqlDB)
	socialWorker := worker.NewSocialWorker(rbq, socialRepo, cache, socialQueue)

	videoRepo := video.NewVideoRepository(sqlDB)

	likeRepo := video.NewLikeRepository(sqlDB)
	likeWorker := worker.NewLikeWorker(rbq, likeRepo, videoRepo, likeQueue)

	commentRepo := video.NewCommentRepository(sqlDB)
	commentWorker := worker.NewCommentWorker(rbq, commentRepo, videoRepo, commentQueue)

	var popularityWorker *worker.PopularityWorker
	if cache != nil {
		popularityWorker = worker.NewPopularityWorker(rbq, cache, popularityQueue)
	}

	// FanoutWorker：消费 FanoutMQ，根据作者粉丝数决定推/拉策略
	var fanoutWorker *worker.FanoutWorker
	if cache != nil {
		fanoutWorker = worker.NewFanoutWorker(rbq, socialRepo, cache, fanoutQueue)
	}

	// 创建一个上下文管理消费者
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop() // 自动关闭上下文

	// 启动一下pprof服务连接
	pprofServer, err := observability.NewPprofServer("Worker", cfg.ObservabilityConfig.Pprof.Enabled, cfg.ObservabilityConfig.Pprof.WorkerAddr)
	if err != nil {
		log.Printf("Worker pprof server start failed: %v", err)
	}
	defer pprofServer.Close()

	// 开始启动消费者协程
	errCh := make(chan error, 5) // 创建一个管道接收协程的错误信息
	log.Printf("Worker started consuming queue = %s", socialQueue)
	go func() {
		errCh <- socialWorker.Run(ctx)
	}()
	log.Printf("Worker started consuming queue = %s", likeQueue)
	go func() {
		errCh <- likeWorker.Run(ctx)
	}()
	log.Printf("Worker started consuming queue = %s", commentQueue)
	go func() {
		errCh <- commentWorker.Run(ctx)
	}()
	if popularityWorker != nil {
		log.Printf("Worker started consuming queue = %s", popularityQueue)
		go func() {
			errCh <- popularityWorker.Run(ctx)
		}()
	}
	if fanoutWorker != nil {
		log.Printf("Worker started consuming queue = %s (push-pull hybrid)", fanoutQueue)
		go func() {
			errCh <- fanoutWorker.Run(ctx)
		}()
	}

	err = <-errCh
	if err != nil && err != context.Canceled {
		log.Fatalf("Worker stopped: %v", err)
	}
	log.Printf("Worker stopped")
}

func declareSocialTopology(rbq *rabbitmq.RabbitMQ) error {
	return rbq.DeclareTopic(socialExchange, socialQueue, socialBindingKey)
}

func declareLikeTopology(rbq *rabbitmq.RabbitMQ) error {
	return rbq.DeclareTopic(likeExchange, likeQueue, likeBindingKey)
}

func declareCommentTopology(rbq *rabbitmq.RabbitMQ) error {
	return rbq.DeclareTopic(commentExchange, commentQueue, commentBindingKey)
}

func declarePopularityTopology(rbq *rabbitmq.RabbitMQ) error {
	return rbq.DeclareTopic(popularityExchange, popularityQueue, popularityBindingKey)
}

func declareFanoutTopology(rbq *rabbitmq.RabbitMQ) error {
	return rbq.DeclareTopic(fanoutExchange, fanoutQueue, fanoutBindingKey)
}
