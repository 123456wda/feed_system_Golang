package video

import "time"

type Video struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	AuthorID    uint      `gorm:"index;not null" json:"author_id"`
	Username    string    `gorm:"type:varchar(255);not null" json:"username"`
	Title       string    `gorm:"type:varchar(255);not null" json:"title"`
	Description string    `gorm:"type:varchar(255);" json:"description,omitempty"`
	PlayURL     string    `gorm:"type:varchar(255);not null" json:"play_url"`
	CoverURL    string    `gorm:"type:varchar(255);not null" json:"cover_url"`
	CreateTime  time.Time `gorm:"autoCreateTime" json:"create_time"`
	LikesCount  int64     `gorm:"column:likes_count;not null;default:0" json:"likes_count"`
	Popularity  int64     `gorm:"column:popularity;not null;default:0" json:"popularity"`
}

// OutboxMsg 是本地消息表，用来暂存待发送的发布事件，保证视频入库后能可靠地异步投递到 MQ。
type OutboxMsg struct {
	ID         uint      `gorm:"primaryKey"`
	VideoID    uint      `gorm:"index"`
	AuthorID   uint      `gorm:"index"` // 视频作者 ID，用于 fanout 判断是否大V
	EventType  string    `gorm:"type:varchar(50)"`
	CreateTime time.Time `gorm:"autoCreateTime"`
	Status     string    `gorm:"type:varchar(50);index"`
}

// 处理根据Authorid罗列视频的请求
type ListByAuthorIDRequest struct {
	AuthorID uint `json:"author_id" binding:"required"`
}

// 处理getDetail请求的参数
type GetDetailRequest struct {
	ID uint `json:"id" binding:"required"`
}

// 处理发布视频参数的结构体
type PublishVideoRequest struct {
	Title       string `json:"title" binding:"required"`
	Description string `json:"description"`
	PlayURL     string `json:"play_url" binding:"required"`
	CoverURL    string `json:"cover_url" binding:"required"`
}

// 处理删除视频请求的结构体
type DeleteVideoRequest struct {
	ID uint `json:"id" binding:"required"`
}

// 处理更新点赞数请求的结构体，用于设置绝对值（区别于 ChangeLikesCount 的增量更新）
type UpdateLikesCountRequest struct {
	ID         uint  `json:"id" binding:"required"`
	LikesCount int64 `json:"likes_count" binding:"required"`
}
