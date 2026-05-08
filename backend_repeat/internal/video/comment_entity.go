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
	VideoID uint `json:"video_id" binding:"required"`
}

type PublishCommentRequest struct {
	Content string `json:"content" binding:"required"`
	VideoID uint   `json:"video_id" binding:"required"`
}

// 处理删除评论请求的结构体
type DeleteCommentRequest struct {
	CommentID uint `json:"comment_id" binding:"required"`
}
