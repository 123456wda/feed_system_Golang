package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/db"
	apphttp "feedsystem_video_go/internal/http"
	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/observability"
)

func main() {
	// Load config.
	configPath := os.Getenv("CONFIG_PATH")
	configPath_docker := flag.String("config", "", "配置文件路径")
	flag.Parse()
	configPath = *configPath_docker
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
	log.Printf("mysql connected")
	defer db.CloseDB(sqlDB)

	// Connect Redis cache.
	cache, err := rediscache.NewFromEnv(&cfg.Redis)
	if err != nil {
		log.Printf("Failed to connect to redis: %v", err)
		cache = nil
	} else {
		// Verify Redis connection.
		Pingctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	rbq, err := rabbitmq.NewRabbitMQ(&cfg.RabbitMQ)
	if err != nil {
		log.Printf("Failed to connect to RabbitMQ: %v", err)
		rbq = nil
	} else {
		defer rbq.Close()
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
	r := apphttp.SetRouter(sqlDB, cache, rbq)
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// 异步启动服务
	go apphttp.StartServer(srv, &cfg.Server)

	// 优雅停机
	apphttp.GracefulShutdown(srv)
}
