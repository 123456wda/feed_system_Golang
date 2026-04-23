package main

import (
	"fmt"
	"log"
	"os"

	"feedsystem_video_go/internal/config"
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
}
