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
	"log"
	"mime/quotedprintable"
	"os"
	"os/signal"
	"strconv"
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

	amqp "github.com/rabbitmq/amqp091-go"
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

	prefetchCount  = 50
	prefetchSize   = 0
	prefetchGlobal = false
)

func main() {
	// Read config.
	configPath := os.Getenv("CONFIG_PATH")
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
		pingCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
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
		log.Fatalf("Failed to initialze RabbitMQ: %w", err)
	}
	defer rbq.Close()

	// Declare MQ topology.
	if err := declareSocialTopology(rbq); err != nil {
		log.Fatalf("Failed to declare social topology: %w", err)
	}
	if err := declareLikeTopology(rbq); err != nil {
		log.Fatalf("Failed to declare like topology: %w", err)
	}
	if err := declareCommentTopology(rbq); err != nil {
		log.Fatalf("Failed to declare comment topology: %w", err)
	}
	if cache != nil {
		if err := declarePopularityTopology(rbq); err != nil {
			log.Fatalf("Failed to declare popularity topology: %w", err)
		}
	}

	// Configure consumer prefetch.
	if err := rbq.Ch.Qos(prefetchCount, prefetchSize, prefetchGlobal); err != nil {
		log.Fatalf("Failed to set Qos: %w", err)
	}

	// Create repositories and workers.
	socialRepo := social.NewSocialRepository(sqlDB)
	socialWorker := worker.NewSocialWorker(rbq, socialRepo, socialQueue)

	videoRepo := video.NewVideoRepository(sqlDB)

	likeRepo := video.NewLikeRepository(sqlDB)
	likeWorker := worker.NewLikeWorker(rbq, likeRepo, videoRepo, likeQueue)

	commentRepo := video.NewCommentRepository(sqlDB)
	commentWorker := worker.NewCommentWorker(rbq, commentRepo, videoRepo, commentQueue)

	if cache != nil {
		popularityWorker := worker.NewPopularityWorker(rbq, cache, popularityQueue)
	}
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
