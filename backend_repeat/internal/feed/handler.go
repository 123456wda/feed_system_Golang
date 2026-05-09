package feed

import (
	"net/http"
	"time"

	"feedsystem_video_go/internal/middleware/jwt"

	"github.com/gin-gonic/gin"
)

// FeedHandler 处理 feed 流相关的 HTTP 请求。
// listLatest / listLikesCount / listByPopularity 为公共接口（SoftJWTAuth），
// listByFollowing 需要登录（JWTAuth）。
type FeedHandler struct {
	service *FeedService
}

func NewFeedHandler(service *FeedService) *FeedHandler {
	return &FeedHandler{service: service}
}

// ListLatest 最新视频流接口，POST /feed/listLatest。
// 支持游标分页：latest_time 为上一页最后一条的 create_time 毫秒时间戳。
func (f *FeedHandler) ListLatest(c *gin.Context) {
	// TODO: 解析请求 → 调 service.ListLatest → 返回
	var req ListLatestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Limit <= 0 || req.Limit > 100 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be between 1 and 100"})
		return
	}
	viewerAccountID, err := jwt.GetAccountID(c)
	if err != nil {
		viewerAccountID = 0
	}

	var latestTime time.Time
	if req.LatestTime > 0 {
		latestTime = time.UnixMilli(req.LatestTime)
	}
	feedItems, err := f.service.ListLatest(c.Request.Context(), req.Limit, latestTime, viewerAccountID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, feedItems)
}

// ListLikesCount 按点赞数排序接口，POST /feed/listLikesCount。
// 双字段游标分页：likes_count_before 和 id_before 必须同时提供。
func (f *FeedHandler) ListLikesCount(c *gin.Context) {
	var req ListLikesCountRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = 10
	}
	// 校验双字段游标：必须同时提供或同时不提供
	var cursor *LikesCountCursor
	if req.LikesCountBefore != nil || req.IDBefore != nil {
		if req.LikesCountBefore == nil || req.IDBefore == nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "likes_count_before and id_before must be provided together"})
			return
		}
		if *req.LikesCountBefore < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cursor: likes_count_before must be >= 0"})
			return
		}
		if *req.IDBefore == 0 && *req.LikesCountBefore != 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cursor: id_before must be > 0"})
			return
		}
		if *req.IDBefore > 0 {
			cursor = &LikesCountCursor{LikesCount: *req.LikesCountBefore, ID: *req.IDBefore}
		}
	}
	viewerAccountID, _ := jwt.GetAccountID(c)
	feedItems, err := f.service.ListLikesCount(c.Request.Context(), req.Limit, cursor, viewerAccountID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, feedItems)
}

// ListByFollowing 关注的人的视频接口，POST /feed/listByFollowing。
// 需要 JWT 登录，viewerAccountID 从 token 获取。
func (f *FeedHandler) ListByFollowing(c *gin.Context) {
	var req ListByFollowingRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = 10
	}
	viewerAccountID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	var latestTime time.Time
	if req.LatestTime > 0 {
		latestTime = time.Unix(req.LatestTime, 0)
	}
	feedItems, err := f.service.ListByFollowing(c.Request.Context(), req.Limit, latestTime, viewerAccountID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, feedItems)
}

// ListByPopularity 热度排行接口，POST /feed/listByPopularity。
// as_of + offset 稳定分页，Redis 热榜优先，降级 MySQL。
func (f *FeedHandler) ListByPopularity(c *gin.Context) {
	var req ListByPopularityRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = 10
	}
	viewerAccountID, _ := jwt.GetAccountID(c)
	if req.LatestPopularity < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "latest_popularity must be >= 0"})
		return
	}
	var latestPopularity int64
	var latestBefore time.Time
	var latestIDBefore uint
	anyCursor := !req.LatestBefore.IsZero() || req.LatestIDBefore != nil
	if anyCursor {
		if req.LatestBefore.IsZero() || req.LatestIDBefore == nil || *req.LatestIDBefore == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "latest_before and latest_id_before must be provided together"})
			return
		}
		latestPopularity = req.LatestPopularity
		latestBefore = req.LatestBefore
		latestIDBefore = *req.LatestIDBefore
	}
	resp, err := f.service.ListByPopularity(
		c.Request.Context(),
		req.Limit,
		req.AsOf,
		req.Offset,
		viewerAccountID,
		latestPopularity,
		latestBefore,
		latestIDBefore,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, resp)
}
