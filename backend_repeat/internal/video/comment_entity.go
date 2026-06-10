package video

import "time"

type Comment struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	Username  string    `gorm:"index" json:"username"`
	VideoID   uint      `gorm:"index" json:"video_id"`
	AuthorID  uint      `gorm:"index" json:"author_id"`
	Content   string    `gorm:"type:text" json:"content"`
	CreatedAt time.Time `gorm:"autoCreateTime" json:"created_at"`
}

type GetAllCommentsRequest struct {
	VideoID  uint `json:"video_id" binding:"required"`
	Page     int  `json:"page"`      // 页码，从 1 开始，默认 1
	PageSize int  `json:"page_size"` // 每页条数，默认 20，最大 100
}

type PublishCommentRequest struct {
	Content string `json:"content" binding:"required"`
	VideoID uint   `json:"video_id" binding:"required"`
}

// 处理删除评论请求的结构体
type DeleteCommentRequest struct {
	CommentID uint `json:"comment_id" binding:"required"`
}
