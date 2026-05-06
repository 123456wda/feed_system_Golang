package video

import "time"

// Like 表示用户对视频的点赞记录。
// VideoID + AccountID 构成联合唯一索引，保证同一用户对同一视频只能点赞一次。
type Like struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	VideoID   uint      `gorm:"uniqueIndex:idx_like_video_account;not null" json:"video_id"`
	AccountID uint      `gorm:"uniqueIndex:idx_like_video_account;not null" json:"account_id"`
	CreatedAt time.Time `json:"created_at"`
}

// LikeRequest 是点赞/取消点赞/判断是否点赞 共用请求体。
// 前端只需要传 video_id，用户 ID 从 JWT 中提取，不信任客户端传参。
type LikeRequest struct {
	VideoID uint `json:"video_id"`
}
