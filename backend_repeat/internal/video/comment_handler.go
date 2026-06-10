package video

import (
	"net/http"

	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/middleware/jwt"

	"github.com/gin-gonic/gin"
)

type CommentHandler struct {
	CommentService *CommentService
	AccountService *account.AccountService
}

func NewCommentHandler(ser *CommentService, accountService *account.AccountService) *CommentHandler {
	return &CommentHandler{CommentService: ser, AccountService: accountService}
}

func (h *CommentHandler) PublishComment(c *gin.Context) {
	var req PublishCommentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.VideoID <= 0 || req.Content == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "video_id and content are invalid"})
		return
	}

	userId, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	user, err := h.AccountService.FindByID(c.Request.Context(), userId)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return

	}
	comment := &Comment{
		Username: user.Username,
		VideoID:  req.VideoID,
		AuthorID: userId,
		Content:  req.Content,
	}
	if err := h.CommentService.Publish(c.Request.Context(), comment); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "comment published successfully"})
}

// DeleteComment 删除评论接口，POST /comment/delete，需要 JWT 登录。
// 流程：解析请求体 → 从 JWT 获取当前用户 ID → 校验权限并删除。
func (h *CommentHandler) DeleteComment(c *gin.Context) {
	var req DeleteCommentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	accountID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.CommentService.Delete(c.Request.Context(), req.CommentID, accountID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "comment deleted successfully"})
}

func (h *CommentHandler) GetAllComments(c *gin.Context) {
	var req GetAllCommentsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.VideoID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "video_id is required"})
		return
	}
	comments, err := h.CommentService.GetAll(c.Request.Context(), req.VideoID, req.Page, req.PageSize)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, comments)
}
