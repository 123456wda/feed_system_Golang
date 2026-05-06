package video

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/middleware/jwt"

	"github.com/gin-gonic/gin"
)

type VideoHandler struct {
	s *VideoService
	r *account.AccountRepository
}

func NewVideoHandler(s *VideoService, r *account.AccountRepository) *VideoHandler {
	if s == nil || r == nil {
		log.Printf("NewVideoHandler: invalid parameter")
		return nil
	}
	return &VideoHandler{s, r}
}

func (h *VideoHandler) ListByAuthorID(c *gin.Context) {
	var req ListByAuthorIDRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	videos, err := h.s.ListByAuthorID(c.Request.Context(), req.AuthorID)
	if err != nil {
		if errors.Is(err, ErrInvalidAuthorID) {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	writeListByAuthorIDResponse(c, videos)
}

func writeListByAuthorIDResponse(c *gin.Context, videos []Video) {
	c.JSON(http.StatusOK, videos)
}

func (h *VideoHandler) GetDetail(c *gin.Context) {
	var req GetDetailRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	video, err := h.s.GetDetail(c.Request.Context(), req.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, video)
}

func (h *VideoHandler) UploadVideo(c *gin.Context) {
	// 上传接口挂在 JWT 中间件后面，这里从上下文取当前登录用户 ID。
	// 用户 ID 会参与目录生成，保证不同用户上传的资源天然分目录隔离。
	authorID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 前端使用 multipart/form-data 上传文件，字段名必须是 file。
	// 如果字段名不匹配，Gin 会返回错误，这里统一提示 missing file。
	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing file"})
		return
	}

	// 视频文件体积较大，这里限制为 200MB，避免单次上传占满磁盘或内存。
	const maxSize = 200 << 20
	if file.Size <= 0 || file.Size > maxSize {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid file size"})
		return
	}

	// 这里只按扩展名做第一层限制：统一转小写后只允许 mp4。
	// 更严格的 MIME/文件头校验可以后续补充在这一层。
	ext := strings.ToLower(filepath.Ext(file.Filename))
	if ext != ".mp4" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "only .mp4 is allowed"})
		return
	}

	// 目录结构：.run/uploads/videos/{用户ID}/{日期}/文件名。
	// 这样既方便按用户查找，也能避免同一目录下文件过多。
	date := time.Now().Format("20060102")
	relDir := filepath.Join("videos", fmt.Sprintf("%d", authorID), date)
	root := filepath.Join(".run", "uploads")
	absDir := filepath.Join(root, relDir)
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 不使用用户原始文件名，避免重名覆盖和暴露本地文件名信息。
	filename := randHex(16) + ext
	absPath := filepath.Join(absDir, filename)

	// 真正把 multipart 里的临时文件保存到后端本地磁盘。
	if err := c.SaveUploadedFile(file, absPath); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 对外访问路径必须使用 URL 风格的 /，因此这里用 path.Join 而不是 filepath.Join。
	// router 中通过 r.Static("/static", "./.run/uploads") 把本地目录暴露出去。
	urlPath := path.Join("/static", "videos", fmt.Sprintf("%d", authorID), date, filename)

	c.JSON(http.StatusOK, gin.H{
		"url":      buildAbsoluteURL(c, urlPath),
		"play_url": buildAbsoluteURL(c, urlPath),
	})
}

func (h *VideoHandler) UploadCover(c *gin.Context) {
	// 上传封面也必须知道当前登录用户，目录会按用户 ID 分层保存。
	authorID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 前端同样通过 multipart/form-data 的 file 字段传封面图片。
	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing file"})
		return
	}

	// 封面图比视频小很多，限制为 10MB。
	const maxSize = 10 << 20
	if file.Size <= 0 || file.Size > maxSize {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid file size"})
		return
	}

	// 封面允许常见 Web 图片格式；扩展名统一转小写，兼容 PNG/JPG 这类大写后缀。
	ext := strings.ToLower(filepath.Ext(file.Filename))
	switch ext {
	case ".jpg", ".jpeg", ".png", ".webp":
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "only .jpg/.jpeg/.png/.webp is allowed"})
		return
	}

	// 目录结构：.run/uploads/covers/{用户ID}/{日期}/文件名。
	date := time.Now().Format("20060102")
	relDir := filepath.Join("covers", fmt.Sprintf("%d", authorID), date)
	root := filepath.Join(".run", "uploads")
	absDir := filepath.Join(root, relDir)
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 使用随机文件名，保留合法扩展名，方便浏览器和静态服务识别类型。
	filename := randHex(16) + ext
	absPath := filepath.Join(absDir, filename)

	// 保存封面图片到后端本地磁盘。
	if err := c.SaveUploadedFile(file, absPath); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 生成静态访问地址，后续 publish 接口会把这个 cover_url 写入 videos 表。
	urlPath := path.Join("/static", "covers", fmt.Sprintf("%d", authorID), date, filename)

	c.JSON(http.StatusOK, gin.H{
		"url":       buildAbsoluteURL(c, urlPath),
		"cover_url": buildAbsoluteURL(c, urlPath),
	})
}

func (h *VideoHandler) PublishVideo(c *gin.Context) {
	var req PublishVideoRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// 参数校验完毕
	authorID, err := jwt.GetAccountID(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	userName, err := jwt.GetUsername(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// 组装结构体
	video := &Video{
		AuthorID:    authorID,
		Username:    userName,
		Title:       req.Title,
		Description: req.Description,
		PlayURL:     req.PlayURL,
		CoverURL:    req.CoverURL,
		CreateTime:  time.Now(),
	}
	if err := h.s.PublishVideo(c.Request.Context(), video); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return

	}
	c.JSON(http.StatusOK, video)
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func buildAbsoluteURL(c *gin.Context, p string) string {
	scheme := "http"
	if c.Request.TLS != nil {
		scheme = "https"
	}
	if forwardedProto := c.GetHeader("X-Forwarded-Proto"); forwardedProto != "" {
		scheme = forwardedProto
	}
	return fmt.Sprintf("%s://%s%s", scheme, c.Request.Host, p)
}
