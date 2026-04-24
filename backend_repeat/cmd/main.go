package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/db"
	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/observability"
)

func main() {
	// Load config.
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "configs/config.yaml"
	}
	log.Printf("loading config from %s", configPath)

	// Try to load config from file.
	cfg, useDefault, err := config.LoadLocalDev(configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	if useDefault {
		log.Printf("Config File %s not found, using default local config", configPath)
	} else {
		log.Printf("config loaded from file: %s", configPath)
	}

	fmt.Println(cfg)

	// Connect database.
	sqlDB, err := db.NewDB(cfg.Database)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Auto-migrate tables.
	err = db.AutoMigrate(sqlDB)
	if err != nil {
		log.Fatalf("Failed to AutoMigrate database: %v", err)
	}
	defer db.CloseDB(sqlDB)

	// Connect Redis cache.
	cache, err := rediscache.NewFromEnv(&cfg.Redis)
	if err != nil {
		log.Printf("Failed to connect to redis: %v", err)
		cache = nil
	} else {
		// Verify Redis connection.
		Pingctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()

		if err := cache.Ping(Pingctx); err != nil {
			log.Printf("redis not available: %v", err)
			_ = cache.Close()
			cache = nil
		} else {
			defer cache.Close()
			log.Println("redis connected")
		}
	}

	// Connect RabbitMQ.
	rmq, err := rabbitmq.NewRabbitMQ(&cfg.RabbitMQ)
	if err != nil {
		log.Printf("Failed to connect to RabbitMQ: %v", err)
		rmq = nil
	} else {
		defer rmq.Close()
		log.Printf("rabbitMQ connected")
	}

	// Start pprof server.
	pprofServer, err := observability.NewPprofServer(
		"API",
		cfg.ObservabilityConfig.Pprof.Enabled,
		cfg.ObservabilityConfig.Pprof.ApiAddr,
	)
	if err != nil {
		log.Printf("Failed to start API pprof server: %v", err)
	}
	defer pprofServer.Close()

	// Set routes.
}
