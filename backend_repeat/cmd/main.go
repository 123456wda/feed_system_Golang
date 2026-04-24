package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"feedsystem_video_go/db"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/observability"
)

func main() {
	// 加载配置
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "configs/config.yaml"
	}
	log.Printf("loading config from %s", configPath)

	// 尝试从文件里面读取配置
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

	// 连接数据库
	sqlDB, err := db.NewDB(cfg.Database)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// 自动化建表
	err = db.AutoMigrate(sqlDB)
	if err != nil {
		log.Fatalf("Failed to AutoMigrate database: %v", err)
	}
	defer db.CloseDB(sqlDB)

	// 连接Redis缓存
	cache, err := redis.NewFromEnv(&cfg.Redis)
	if err != nil {
		log.Printf("Failed to connect to redis: %v", err)
		cache = nil
	} else {
		// 测试连接是否成功
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

	// 连接消息队列

	// 启动pprof性能分析服务
	pprofServer, err := observability.NewPprofServer(
		"API",
		cfg.ObservabilityConfig.Pprof.Enabled,
		cfg.ObservabilityConfig.Pprof.ApiAddr,
	)
	if err != nil {
		log.Printf("Failed to start API pprof server: %v", err)
	}
	defer pprofServer.Close()

	// 设置路由

}
