package video

import (
	"context"
	"fmt"
	"strconv"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"
)

// UpdatePopularityCache 更新视频热度相关的 Redis 缓存。
// 在点赞/取消点赞/评论等操作后调用，做两件事：
//   1. 删除视频详情缓存，让下次 GetDetail 命中 DB 拿到最新计数
//   2. 更新每分钟窗口的有序集合，用于 feed 流按热度排行
func UpdatePopularityCache(ctx context.Context, cache *rediscache.Client, videoID uint, change int64) {
	if cache == nil || videoID == 0 || change == 0 {
		return
	}

	// 删除详情缓存，避免读到过期的 likes_count / popularity
	_ = cache.Del(context.Background(), fmt.Sprintf("video:detail:id=%d", videoID))

	// 构造分钟级窗口 key：hot:video:1m:202605061435
	now := time.Now().UTC().Truncate(time.Minute)
	windowKey := "hot:video:1m:" + now.Format("200601021504")
	member := strconv.FormatUint(uint64(videoID), 10)

	// Redis 操作用独立超时，避免被请求 context 取消连带
	opCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// 增加视频在热度窗口内的分数
	_ = cache.ZincrBy(opCtx, windowKey, member, float64(change))
	// 窗口 key 2 小时后自动过期，防止内存泄漏
	_ = cache.Expire(opCtx, windowKey, 2*time.Hour)
}
