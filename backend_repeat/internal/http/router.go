package http

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/feed"
	"feedsystem_video_go/internal/middleware/jwt"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/middleware/ratelimit"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	"feedsystem_video_go/internal/worker"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetRouter(db *gorm.DB, cache *rediscache.Client, rbq *rabbitmq.RabbitMQ) *gin.Engine {
	r := gin.Default()
	// 不信任任何代理的转发ip,直接取到实际的tcp连接ip
	if err := r.SetTrustedProxies(nil); err != nil {
		log.Printf("SetTrustedProxies failed: %v", err)
	}
	// 把URL路径/static映射为对应路径,目的是方便加载本地视频资源
	r.Static("/static", "./.run/uploads")

	// 创建account相关限流器
	loginLimiter := ratelimit.Limit(cache, "account_login", 10, time.Minute, ratelimit.KeyByIP)
	registerLimiter := ratelimit.Limit(cache, "account_register", 10, time.Minute, ratelimit.KeyByIP)

	// 创建account相关业务路由组以及路由
	/*
		这里涉及一个层层封装
		最底层封装account仓库,操作数据层
		外包一层service,负责处理业务逻辑
		最外层包一层handler,负责处理响应请求
	*/
	accountRepository := account.NewAccountRepository(db)
	accountService := account.NewAccountService(accountRepository, cache)
	accountHandler := account.NewAccountHandler(accountService)
	accountGroup := r.Group("/account")
	{
		accountGroup.POST("/register", registerLimiter, accountHandler.CreateAccount)
		accountGroup.POST("/login", loginLimiter, accountHandler.Login)
		accountGroup.POST("/changePassword", accountHandler.ChangePassword)
		accountGroup.POST("/findByID", accountHandler.FindByID)
		accountGroup.POST("/findByUsername", accountHandler.FindByUsername)
	}
	protectedAccountGroup := accountGroup.Group("")
	protectedAccountGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedAccountGroup.POST("/logout", accountHandler.Logout)
		protectedAccountGroup.POST("/rename", accountHandler.Rename)
	}

	// 处理video相关业务的路由
	videoRepository := video.NewVideoRepository(db)
	// 在通用rabbitmq客户端连接的基础上封装一层popularity的Topic封装,处于生产者地位
	popularityMQ, err := rabbitmq.NewPopularityMQ(rbq)
	if err != nil {
		log.Printf("PopularityMQ init failed (mq disabled): %v", err)
		popularityMQ = nil
	}
	videoService := video.NewVideoService(videoRepository, cache, popularityMQ)
	videoHandler := video.NewVideoHandler(videoService, accountRepository)
	videoGroup := r.Group("/video")
	{
		videoGroup.POST("/listByAuthorID", videoHandler.ListByAuthorID)
		videoGroup.POST("/getDetail", videoHandler.GetDetail)
	}
	protectedVideoGroup := videoGroup.Group("")
	protectedVideoGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		// 上传视频到后端服务的本地文件夹
		protectedVideoGroup.POST("/uploadVideo", videoHandler.UploadVideo)
		// 上传视频封面到后端服务的本地文件夹
		protectedVideoGroup.POST("/uploadCover", videoHandler.UploadCover)
		// 发布视频,把视频对应元数据存储到后端数据库里面
		protectedVideoGroup.POST("/publish", videoHandler.PublishVideo)
		// 删除视频（仅作者本人可操作）
		protectedVideoGroup.POST("/delete", videoHandler.DeleteVideo)
		// 更新点赞数（供内部同步使用）
		protectedVideoGroup.POST("/updateLikesCount", videoHandler.UpdateLikesCount)
	}

	// 处理like相关业务的路由

	// 点赞写操作限流：每用户每分钟最多 30 次，防止恶意刷赞
	likeLimiter := ratelimit.Limit(cache, "like_write", 30, time.Minute, ratelimit.KeyByAccount)

	// 初始化 LikeMQ 生产者，用于异步投递点赞/取消点赞事件
	likeMQ, err := rabbitmq.NewLikeMQ(rbq)
	if err != nil {
		log.Printf("LikeMQ init failed (mq disabled): %v", err)
		likeMQ = nil
	}
	likeRepository := video.NewLikeRepository(db)
	likeService := video.NewLikeService(likeRepository, videoRepository, cache, likeMQ, popularityMQ)
	likeHandler := video.NewLikeHandler(likeService)
	likeGroup := r.Group("/like")
	protectedLikeGroup := likeGroup.Group("")
	// 所有点赞接口都需要 JWT 认证
	protectedLikeGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		// 点赞操作
		protectedLikeGroup.POST("/like", likeLimiter, likeHandler.Like)
		// 取消点赞操作
		protectedLikeGroup.POST("/unlike", likeLimiter, likeHandler.Unlike)
		// 查询是否已点赞（仅需登录，不限流）
		protectedLikeGroup.POST("/isLiked", likeHandler.IsLiked)
		// 查看我的点赞视频列表（仅需登录，不限流——查询频次低）
		protectedLikeGroup.POST("/listMyLikedVideos", likeHandler.ListMyLikedVideos)
	}

	// 处理comment相关业务的路由

	// 组装一下限流器
	commentLimiter := ratelimit.Limit(cache, "comment_write", 10, time.Minute, ratelimit.KeyByAccount)

	commentRepository := video.NewCommentRepository(db)
	commentMQ, err := rabbitmq.NewCommentMQ(rbq)
	if err != nil {
		log.Printf("CommentMQ init failed (mq disabled): %v", err)
		commentMQ = nil
	}
	commentService := video.NewCommentService(commentRepository, videoRepository, cache, commentMQ, popularityMQ)
	commentHandler := video.NewCommentHandler(commentService, accountService)
	commentGroup := r.Group("/comment")
	{
		commentGroup.POST("/listAll", commentHandler.GetAllComments)
	}
	protectedCommentGroup := commentGroup.Group("")
	protectedCommentGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedCommentGroup.POST("/publish", commentLimiter, commentHandler.PublishComment)
		protectedCommentGroup.POST("/delete", commentLimiter, commentHandler.DeleteComment)
	}

	// ========== social 模块 ==========
	// 关注/取关是写操作，需要限流防止恶意刷关注
	socialLimiter := ratelimit.Limit(cache, "social_write", 20, time.Minute, ratelimit.KeyByAccount)

	// 初始化 SocialMQ 生产者，用于异步投递关注/取关事件。
	// MQ 通知下游（如 timeline fanout），但关注关系本身由 API 同步写库保证。
	// MQ 不可用时降级为纯同步模式，不影响核心关注功能。
	socialMQ, err := rabbitmq.NewSocialMQ(rbq)
	if err != nil {
		log.Printf("SocialMQ init failed (mq disabled): %v", err)
		socialMQ = nil
	}
	// 依赖注入：repo → service → handler，保持分层清晰
	socialRepository := social.NewSocialRepository(db)
	socialService := social.NewSocialService(socialRepository, accountRepository, socialMQ)
	socialHandler := social.NewSocialHandler(socialService)
	socialGroup := r.Group("/social")
	protectedSocialGroup := socialGroup.Group("")
	// 所有关注接口都需要 JWT 认证（必须登录才能操作关注关系）
	protectedSocialGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		// 关注博主（写操作，限流）
		protectedSocialGroup.POST("/follow", socialLimiter, socialHandler.Follow)
		// 取消关注（写操作，限流）
		protectedSocialGroup.POST("/unfollow", socialLimiter, socialHandler.Unfollow)
		// 获取某博主的粉丝列表（查询接口，不限流；不传 vlogger_id 则查自己）
		protectedSocialGroup.POST("/getAllFollowers", socialHandler.GetAllFollowers)
		// 获取某用户的关注列表（查询接口，不限流；不传 follower_id 则查自己）
		protectedSocialGroup.POST("/getAllVloggers", socialHandler.GetAllVloggers)
	}

	// ========== feed 模块 ==========
	// 依赖 likeRepository（已在 like 模块初始化），用于批量查 is_liked 状态
	feedRepository := feed.NewFeedRepository(db)
	feedService := feed.NewFeedService(feedRepository, likeRepository, cache)
	feedHandler := feed.NewFeedHandler(feedService)
	feedGroup := r.Group("/feed")
	// 公共接口使用 SoftJWTAuth：有 token 解析身份（用于 is_liked），没有也不拦截
	feedGroup.Use(jwt.SoftJWTAuth(accountRepository, cache))
	{
		feedGroup.POST("/listLatest", feedHandler.ListLatest)
		feedGroup.POST("/listLikesCount", feedHandler.ListLikesCount)
		feedGroup.POST("/listByPopularity", feedHandler.ListByPopularity)
	}
	protectedFeedGroup := feedGroup.Group("")
	protectedFeedGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedFeedGroup.POST("/listByFollowing", feedHandler.ListByFollowing)
	}

	// ========== worker 模块 ==========
	// TimelineMQ：视频发布事件的消息通道
	timelineMQ, err := rabbitmq.NewTimelineMQ(rbq)
	if err != nil {
		log.Printf("timelineMQ init failed (mq disabled): %v", err)
		timelineMQ = nil
	}
	// OutboxWorker：轮询 outbox 表，投递未发送的事件到 TimelineMQ
	worker.StartOutboxPoller(db, timelineMQ)
	// Consumer：消费 TimelineMQ，写入 Redis ZSET (feed:global_timeline)
	worker.StartConsumer(timelineMQ, "video.timeline.update.queue", cache)

	return r
}

func StartServer(srv *http.Server, cfg *config.ServerConfig) {
	log.Printf("api server started at :%d", cfg.Port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen server failed: %v", err)
	}
}

func GracefulShutdown(srv *http.Server) {
	// 监听关闭型号
	stop := make(chan os.Signal, 1)

	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	// 直到接收到信号才会继续执行
	<-stop
	log.Printf("api server is shutting down...")

	// 设置超时时间,走完正在进行的请求
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("api server shutdown failed: %v", err)
	}
	log.Printf("server exited gracefully")
}
