package video

import (
	"context"

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
