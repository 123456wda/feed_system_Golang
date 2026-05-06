package video

import (
	"net/http"

	"feedsystem_video_go/internal/middleware/jwt"

	"github.com/gin-gonic/gin"
)

// LikeHandler 处理 /like 路由组的所有请求。
// 所有接口（除了 isLiked/listMyLikedVideos 仅需登录）均需 JWT 认证 + 限流。
type LikeHandler struct {
	service *LikeService
}

func NewLikeHandler(service *LikeService) *LikeHandler {
	return &LikeHandler{service: service}
}

// Like 点赞接口  POST /like/like
// 请求体：{ "video_id": 123 }
// 用户 ID 从 JWT 中提取，不信任客户端传参。
func (h *LikeHandler) Like(c *gin.Context) {
	// 解析请求体
	var req LikeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.VideoID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "video_id is required"})
		return
	}

	// 从 JWT 获取当前登录用户 ID
	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 调用 service 执行点赞
	if err := h.service.Like(c.Request.Context(), accountID, req.VideoID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "like success"})
}

// Unlike 取消点赞接口  POST /like/unlike
// 请求体：{ "video_id": 123 }
func (h *LikeHandler) Unlike(c *gin.Context) {
	// 解析请求体
	var req LikeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.VideoID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "video_id is required"})
		return
	}

	// 从 JWT 获取当前登录用户 ID
	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 调用 service 执行取消点赞
	if err := h.service.Unlike(c.Request.Context(), accountID, req.VideoID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "unlike success"})
}

// IsLiked 查询是否已点赞  POST /like/isLiked
// 请求体：{ "video_id": 123 }
// 返回：{ "is_liked": true/false }
func (h *LikeHandler) IsLiked(c *gin.Context) {
	// 解析请求体
	var req LikeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.VideoID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "video_id is required"})
		return
	}

	// 从 JWT 获取当前登录用户 ID
	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 查询点赞状态
	isLiked, err := h.service.IsLiked(c.Request.Context(), req.VideoID, accountID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"is_liked": isLiked})
}

// ListMyLikedVideos 查看我的点赞列表  POST /like/listMyLikedVideos
// 无需请求体，用户 ID 从 JWT 中提取。
// 返回：该用户点赞过的所有视频列表（按点赞时间倒序）。
func (h *LikeHandler) ListMyLikedVideos(c *gin.Context) {
	// 从 JWT 获取当前登录用户 ID
	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 查询点赞视频列表
	videos, err := h.service.ListLikedVideos(c.Request.Context(), accountID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, videos)
}
