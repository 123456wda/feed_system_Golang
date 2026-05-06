package video

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

type VideoRepository struct {
	db *gorm.DB
}

func NewVideoRepository(db *gorm.DB) *VideoRepository {
	return &VideoRepository{db: db}
}

func (r *VideoRepository) ListByAuthorID(ctx context.Context, authorID uint) ([]Video, error) {
	var videos []Video
	if err := r.db.WithContext(ctx).Model(&Video{}).Where("author_id=?", authorID).Order("create_time desc").Offset(0).Find(&videos).Error; err != nil {
		return nil, err
	}
	return videos, nil
}

func (r *VideoRepository) GetDetail(ctx context.Context, id uint) (*Video, error) {
	var video Video
	if err := r.db.WithContext(ctx).First(&video, id).Error; err != nil {
		return (*Video)(nil), err
	}
	return &video, nil
}

func (r *VideoRepository) PublishVideo(ctx context.Context, video *Video) error {
	err := r.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
		if err := db.Create(video).Error; err != nil {
			return err
		}
		msg := OutboxMsg{
			VideoID:    video.ID,
			EventType:  "video_published",
			Status:     "pending",
			CreateTime: video.CreateTime,
		}
		if err := db.Create(&msg).Error; err != nil {
			return err
		}
		return nil
	})

	return err
}

// IsExist 检查视频是否存在，用于点赞/评论前校验目标视频是否有效。
func (r *VideoRepository) IsExist(ctx context.Context, id uint) (bool, error) {
	var video Video
	if err := r.db.WithContext(ctx).First(&video, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ChangeLikesCount 原子更新点赞数（可增可减），用 GREATEST 兜底防止减成负数。
// 事务中调用时需传入 tx 而不是 r.db。
func (r *VideoRepository) ChangeLikesCount(ctx context.Context, tx *gorm.DB, id uint, change int64) error {
	db := tx
	if db == nil {
		db = r.db
	}
	return db.WithContext(ctx).Model(&Video{}).
		Where("id = ?", id).
		UpdateColumn("likes_count", gorm.Expr("GREATEST(likes_count + ?, 0)", change)).Error
}

// ChangePopularity 原子更新热度值（点赞+1/取消点赞-1/评论+1 等触发）。
// 事务中调用时传入 tx，非事务场景传 nil 即可。
func (r *VideoRepository) ChangePopularity(ctx context.Context, tx *gorm.DB, id uint, change int64) error {
	db := tx
	if db == nil {
		db = r.db
	}
	return db.WithContext(ctx).Model(&Video{}).
		Where("id = ?", id).
		UpdateColumn("popularity", gorm.Expr("GREATEST(popularity + ?, 0)", change)).Error
}
