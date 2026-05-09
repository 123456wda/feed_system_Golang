package social

import (
	"net/http"

	"feedsystem_video_go/internal/middleware/jwt"

	"github.com/gin-gonic/gin"
)

// SocialHandler 处理关注/取关相关的 HTTP 请求。
// 所有接口都需要 JWT 认证（在路由层通过中间件保证）。
type SocialHandler struct {
	service *SocialService
}

func NewSocialHandler(service *SocialService) *SocialHandler {
	return &SocialHandler{service: service}
}

// Follow 关注接口，POST /social/follow，需要 JWT 登录。
// FollowerID 从 JWT 中获取（当前登录用户），VloggerID 从请求体获取。
func (h *SocialHandler) Follow(c *gin.Context) {
	// 解析请求体，获取要关注的博主 ID
	var req FollowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// 从 JWT 上下文获取当前登录用户 ID 作为关注者
	followerID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	// 组装关注关系结构体
	social := &Social{
		FollowerID: followerID,
		VloggerID:  req.VloggerID,
	}
	// 调用 service 层执行关注逻辑
	if err := h.service.Follow(c.Request.Context(), social); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "followed"})
}

// Unfollow 取关接口，POST /social/unfollow，需要 JWT 登录。
func (h *SocialHandler) Unfollow(c *gin.Context) {
	var req UnfollowRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	followerID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	social := &Social{
		FollowerID: followerID,
		VloggerID:  req.VloggerID,
	}
	if err := h.service.Unfollow(c.Request.Context(), social); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "unfollowed"})
}

// GetAllFollowers 获取粉丝列表接口，POST /social/getAllFollowers。
// VloggerID 为空时默认查当前登录用户的粉丝（查自己）。
func (h *SocialHandler) GetAllFollowers(c *gin.Context) {
	var req GetAllFollowersRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	vloggerID := req.VloggerID
	// 未指定博主 ID 时，默认查自己的粉丝
	if vloggerID == 0 {
		accountID, err := jwt.GetAccountID(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}
		vloggerID = accountID
	}

	followers, err := h.service.GetAllFollowers(c.Request.Context(), vloggerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, GetAllFollowersResponse{Followers: followers})
}

// GetAllVloggers 获取关注列表接口，POST /social/getAllVloggers。
// FollowerID 为空时默认查当前登录用户的关注列表（查自己）。
func (h *SocialHandler) GetAllVloggers(c *gin.Context) {
	var req GetAllVloggersRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	followerID := req.FollowerID
	// 未指定用户 ID 时，默认查自己的关注列表
	if followerID == 0 {
		accountID, err := jwt.GetAccountID(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
			return
		}
		followerID = accountID
	}

	vloggers, err := h.service.GetAllVloggers(c.Request.Context(), followerID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, GetAllVloggersResponse{Vloggers: vloggers})
}
