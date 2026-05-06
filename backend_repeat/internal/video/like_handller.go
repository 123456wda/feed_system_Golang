package video

import "github.com/gin-gonic/gin"

type LikeHandler struct {
	LikeService *LikeService
}

func NewLikeHandler(service *LikeService) *LikeHandler {
	return &LikeHandler{LikeService: service}
}

func (h *LikeHandler) Like(c *gin.Context) {
}

func (h *LikeHandler) Unlike(c *gin.Context) {
}

func (h *LikeHandler) IsLiked(c *gin.Context) {
}

func (h *LikeHandler) ListMyLikedVideos(c *gin.Context) {
}
