package db

import (
	"fmt"
	"time"

	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func NewDB(dbcfg config.DatabaseConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		dbcfg.User, dbcfg.Password, dbcfg.Host, dbcfg.Port, dbcfg.DBName)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// 连接池调优
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(100)               // 最大打开连接数
	sqlDB.SetMaxIdleConns(25)                // 最大空闲连接数
	sqlDB.SetConnMaxLifetime(5 * time.Minute) // 连接最大存活时间
	sqlDB.SetConnMaxIdleTime(3 * time.Minute) // 空闲连接最大存活时间

	return db, nil
}

func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(&account.Account{}, &video.Video{}, &video.Like{}, &video.Comment{}, &social.Social{}, &video.OutboxMsg{})
}

func CloseDB(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
