package social

import "feedsystem_video_go/internal/account"

// Social 是关注关系表的 ORM 模型。
// 一条记录表示"FollowerID 关注了 VloggerID"。
// 唯一索引 idx_social_follower_vlogger 保证同一对关系不会重复。
type Social struct {
	ID         uint `gorm:"primaryKey"`
	FollowerID uint `gorm:"not null;index:idx_social_follower;uniqueIndex:idx_social_follower_vlogger"`
	VloggerID  uint `gorm:"not null;index:idx_social_vlogger;uniqueIndex:idx_social_follower_vlogger"`
}

// ========== 请求/响应 DTO ==========

// FollowRequest 关注请求，FollowerID 从 JWT 中获取，不需要前端传。
type FollowRequest struct {
	VloggerID uint `json:"vlogger_id" binding:"required"` // 要关注的博主 ID
}

// UnfollowRequest 取关请求。
type UnfollowRequest struct {
	VloggerID uint `json:"vlogger_id" binding:"required"` // 要取关的博主 ID
}

// GetAllFollowersRequest 获取粉丝列表请求。
// VloggerID 为空时，handler 会默认查当前登录用户的粉丝。
type GetAllFollowersRequest struct {
	VloggerID uint `json:"vlogger_id"` // 博主 ID，可选
}

// GetAllFollowersResponse 粉丝列表响应。
type GetAllFollowersResponse struct {
	Followers []*account.Account `json:"followers"` // 粉丝账号列表
}

// GetAllVloggersRequest 获取关注列表请求。
// FollowerID 为空时，handler 会默认查当前登录用户的关注。
type GetAllVloggersRequest struct {
	FollowerID uint `json:"follower_id"` // 用户 ID，可选
}

// GetAllVloggersResponse 关注列表响应。
type GetAllVloggersResponse struct {
	Vloggers []*account.Account `json:"vloggers"` // 关注的博主列表
}
