package feed

import "time"

// ========== Feed 展示层 DTO ==========

// FeedAuthor feed 流中展示的作者信息。
type FeedAuthor struct {
	ID       uint   `json:"id"`
	Username string `json:"username"`
}

// FeedVideoItem feed 流中展示的视频条目，包含当前用户的点赞状态。
type FeedVideoItem struct {
	ID          uint       `json:"id"`
	Author      FeedAuthor `json:"author"`
	Title       string     `json:"title"`
	Description string     `json:"description,omitempty"`
	PlayURL     string     `json:"play_url"`
	CoverURL    string     `json:"cover_url"`
	CreateTime  int64      `json:"create_time"`
	LikesCount  int64      `json:"likes_count"`
	IsLiked     bool       `json:"is_liked"`
}

// ========== 请求/响应 DTO ==========

// ListLatestRequest 最新视频流请求，时间游标分页。
type ListLatestRequest struct {
	Limit      int   `json:"limit"`
	LatestTime int64 `json:"latest_time"` // 上一页最后一条的 create_time 毫秒时间戳
}

// ListLatestResponse 最新视频流响应。
type ListLatestResponse struct {
	VideoList []FeedVideoItem `json:"video_list"`
	NextTime  int64           `json:"next_time"`
	HasMore   bool            `json:"has_more"`
}

// ListLikesCountRequest 按点赞数排序请求，双字段游标分页。
type ListLikesCountRequest struct {
	Limit            int    `json:"limit"`
	LikesCountBefore *int64 `json:"likes_count_before,omitempty"`
	IDBefore         *uint  `json:"id_before,omitempty"`
}

// LikesCountCursor 按点赞数分页的游标。
type LikesCountCursor struct {
	LikesCount int64
	ID         uint
}

// ListLikesCountResponse 按点赞数排序响应。
type ListLikesCountResponse struct {
	VideoList            []FeedVideoItem `json:"video_list"`
	NextLikesCountBefore *int64          `json:"next_likes_count_before,omitempty"`
	NextIDBefore         *uint           `json:"next_id_before,omitempty"`
	HasMore              bool            `json:"has_more"`
}

// ListByFollowingRequest 关注的人的视频请求。
type ListByFollowingRequest struct {
	Limit      int   `json:"limit"`
	LatestTime int64 `json:"latest_time"`
}

// ListByFollowingResponse 关注的人的视频响应。
type ListByFollowingResponse struct {
	VideoList []FeedVideoItem `json:"video_list"`
	NextTime  int64           `json:"next_time"`
	HasMore   bool            `json:"has_more"`
}

// ListByPopularityRequest 热度排行请求。
// as_of + offset 做稳定分页；latest_* 字段用于 DB 降级时的游标。
type ListByPopularityRequest struct {
	Limit  int   `json:"limit"`
	AsOf   int64 `json:"as_of"`  // 服务器返回的分钟时间戳，第一页传 0
	Offset int   `json:"offset"` // 下一页从这里开始，第一页传 0

	// DB fallback 用（可选）
	LatestPopularity int64     `json:"latest_popularity"`
	LatestBefore     time.Time `json:"latest_before"`
	LatestIDBefore   *uint     `json:"latest_id_before,omitempty"`
}

// ListByPopularityResponse 热度排行响应。
type ListByPopularityResponse struct {
	VideoList  []FeedVideoItem `json:"video_list"`
	AsOf       int64           `json:"as_of"`
	NextOffset int             `json:"next_offset"`
	HasMore    bool            `json:"has_more"`

	// DB fallback 游标（可选）
	NextLatestPopularity *int64     `json:"next_latest_popularity,omitempty"`
	NextLatestBefore     *time.Time `json:"next_latest_before,omitempty"`
	NextLatestIDBefore   *uint      `json:"next_latest_id_before,omitempty"`
}
