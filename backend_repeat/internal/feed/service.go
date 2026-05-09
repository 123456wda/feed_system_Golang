package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/video"

	"github.com/patrickmn/go-cache"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// FeedService 处理 feed 流的业务逻辑。
// 核心能力：冷热分离查询、三级缓存（L1 本地 → L2 Redis → L3 MySQL）、singleflight 防击穿。
type FeedService struct {
	repo         *FeedRepository
	likeRepo     *video.LikeRepository
	rediscache   *rediscache.Client
	localcache   *cache.Cache
	cacheTTL     time.Duration
	requestGroup singleflight.Group
}

func NewFeedService(repo *FeedRepository, likeRepo *video.LikeRepository, rediscache *rediscache.Client) *FeedService {
	// TODO: 初始化 localcache (go-cache)，设置 cacheTTL
	return &FeedService{repo: repo, likeRepo: likeRepo, rediscache: rediscache, localcache: cache.New(3*time.Second, 5*time.Second), cacheTTL: 24 * time.Hour}
}

// ========== 四个对外查询接口 ==========

// ListLatest 最新视频流（冷热分离 + 游标分页）。
// 热数据从 Redis ZSET (feed:global_timeline) 取，不够时自动补冷尾（MySQL）。
// ZSET 为空时 singleflight 防并发重建。
func (f *FeedService) ListLatest(ctx context.Context, limit int, latestBefore time.Time, viewerAccountID uint) (ListLatestResponse, error) {
	// TODO: 实现冷热分离逻辑

	// 获取最老的一条数据
	zsetTail, err := f.rediscache.ZRangeWithScores(ctx, "feed:global_timeline", 0, 0)
	if err != nil {
		return ListLatestResponse{}, err
	}

	isZsetEmpty := len(zsetTail) == 0
	if isZsetEmpty {
		// 需要去MySql里面查找相关数据然后回源Redis
		sfKey := "sf:fallback:feed:global_timeline_rebuild"
		v, err, shared := f.requestGroup.Do(sfKey, func() (interface{}, error) {
			dbVideos, err := f.repo.ListLatest(ctx, 1000, time.Time{})
			if err != nil {
				return nil, err
			}
			if len(dbVideos) == 0 {
				return "EMPTY_DB", nil
			}

			// 回源redis
			redisKey := "feed:global_timeline"
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*2000)
			defer cancel()
			var zElements []redis.Z
			for _, dbVideo := range dbVideos {
				zElements = append(zElements, redis.Z{
					Score:  float64(dbVideo.CreateTime.UnixMilli()),
					Member: fmt.Sprintf("%d", dbVideo.ID),
				})
			}
			f.rediscache.ZAdd(ctx, redisKey, zElements...)
			return "Success", nil
		})
		if err != nil {
			return ListLatestResponse{}, err
		}
		if v == "EMPTY_DB" {
			return ListLatestResponse{HasMore: false}, nil
		}

		if shared == true {
			// 回到redis重新查找一遍
			return f.ListLatest(ctx, limit, latestBefore, viewerAccountID)
		}
	}

	// 此时代表redis里面本身存在数据可以查找
	watermark := int64(zsetTail[0].Score)
	reqTime := time.Now().UnixMilli()
	if !latestBefore.IsZero() {
		reqTime = latestBefore.UnixMilli()
	}

	var baseVideos []*video.Video

	if reqTime <= watermark {
		// 冷查询
		// 针对个别用户的防并发（此时可以用时间戳做锁，因为冷尾流量极小）
		sfKey := fmt.Sprintf("sf:cold:listLatest:%d:%d", limit, reqTime)
		v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
			return f.repo.ListLatest(ctx, limit, latestBefore)
		})
		if err != nil {
			return ListLatestResponse{}, err
		}
		baseVideos = v.([]*video.Video)
		// 不回写 ZSET，防止冷数据污染热点时间线
	} else {
		// 热查询
		// 先在redis里面查找数据
		maxScore := "+inf"
		// 比上次查询的最老的视频还老一点
		if !latestBefore.IsZero() {
			maxScore = fmt.Sprintf("%d", reqTime-1) // 防重复
		}

		videoIDStr, err := f.rediscache.ZRevRangeByScore(ctx, "feed:global_timeline", maxScore, "-inf", 0, int64(limit))
		if err != nil {
			return ListLatestResponse{}, err
		}
		// 先在从字符串获得baseVideos
		var videoIDs []uint
		for _, str := range videoIDStr {
			if id, err := strconv.ParseUint(str, 10, 64); err == nil {
				videoIDs = append(videoIDs, uint(id))
			}
		}
		// 根据id获得BaseVideo

		if len(videoIDs) > 0 {
			baseVideos, err = f.GetVideoByIDs(ctx, videoIDs)
			if err != nil {
				return ListLatestResponse{}, err
			}
		}

		// 判断是否击穿了冷热边界
		if len(baseVideos) < limit {
			remainlimit := limit - len(baseVideos)
			var OldMax time.Time
			if len(baseVideos) == 0 {
				OldMax = latestBefore
			} else {
				OldMax = baseVideos[len(baseVideos)-1].CreateTime
			}

			sfKey := fmt.Sprintf("sf:stitch:listLatest:%d:%d", remainlimit, OldMax.UnixMilli())
			v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
				return f.repo.ListLatest(ctx, remainlimit, OldMax)
			})

			if err == nil {
				coldVideos := v.([]*video.Video)
				baseVideos = append(baseVideos, coldVideos...)
			}

		}

	}
	// 先在获得了基础的video.Video切片,再次封装一下
	var nextTime int64
	if len(baseVideos) > 0 {
		// 将本页最后一条视频的时间作为下一次请求的游标
		nextTime = baseVideos[len(baseVideos)-1].CreateTime.UnixMilli()
	}
	var hasMore bool

	hasMore = len(baseVideos) == limit

	feedVideos, err := f.buildFeedVideos(ctx, baseVideos, viewerAccountID)
	if err != nil {
		return ListLatestResponse{}, err
	}

	return ListLatestResponse{
		VideoList: feedVideos,
		NextTime:  nextTime,
		HasMore:   hasMore,
	}, nil
}

// ListLikesCount 按点赞数排序，纯 MySQL 游标分页。
func (f *FeedService) ListLikesCount(ctx context.Context, limit int, cursor *LikesCountCursor, viewerAccountID uint) (ListLikesCountResponse, error) {
	videos, err := f.repo.ListLikesCountWithCursor(ctx, limit, cursor)
	if err != nil {
		return ListLikesCountResponse{}, err
	}
	hasMore := len(videos) == limit
	feedVideos, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
	if err != nil {
		return ListLikesCountResponse{}, err
	}
	resp := ListLikesCountResponse{
		VideoList: feedVideos,
		HasMore:   hasMore,
	}
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextLikesCountBefore := last.LikesCount
		nextIDBefore := last.ID
		resp.NextLikesCountBefore = &nextLikesCountBefore
		resp.NextIDBefore = &nextIDBefore
	}
	return resp, nil
}

// ListByFollowing 关注的人的视频。
// Redis 缓存 + 分布式锁防击穿，未命中时查 MySQL。
func (f *FeedService) ListByFollowing(ctx context.Context, limit int, latestBefore time.Time, viewerAccountID uint) (ListByFollowingResponse, error) {
	doListByFollowingFromDB := func() (ListByFollowingResponse, error) {
		videos, err := f.repo.ListByFollowing(ctx, limit, viewerAccountID, latestBefore)
		if err != nil {
			return ListByFollowingResponse{}, err
		}
		var nextTime int64
		if len(videos) > 0 {
			nextTime = videos[len(videos)-1].CreateTime.Unix()
		}
		hasMore := len(videos) == limit
		feedVideos, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
		if err != nil {
			return ListByFollowingResponse{}, err
		}
		return ListByFollowingResponse{
			VideoList: feedVideos,
			NextTime:  nextTime,
			HasMore:   hasMore,
		}, nil
	}

	// 尝试从 Redis 缓存读取
	var cacheKey string
	if viewerAccountID != 0 && f.rediscache != nil {
		before := int64(0)
		if !latestBefore.IsZero() {
			before = latestBefore.Unix()
		}
		cacheKey = fmt.Sprintf("feed:listByFollowing:limit=%d:accountID=%d:before=%d", limit, viewerAccountID, before)
		cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		b, err := f.rediscache.GetBytes(cacheCtx, cacheKey)
		if err == nil {
			var cached ListByFollowingResponse
			if err := json.Unmarshal(b, &cached); err == nil {
				return cached, nil
			}
		} else if rediscache.IsMiss(err) {
			// 缓存未命中，尝试加分布式锁
			lockKey := "lock:" + cacheKey
			token, locked, _ := f.rediscache.Lock(cacheCtx, lockKey, 500*time.Millisecond)
			if locked {
				defer func() { _ = f.rediscache.UnLock(context.Background(), lockKey, token) }()
				// double-check：拿到锁后再查一次缓存
				if b, err := f.rediscache.GetBytes(cacheCtx, cacheKey); err == nil {
					var cached ListByFollowingResponse
					if err := json.Unmarshal(b, &cached); err == nil {
						return cached, nil
					}
				}
				// 仍未命中，查 DB 并回写缓存
				resp, err := doListByFollowingFromDB()
				if err != nil {
					return ListByFollowingResponse{}, err
				}
				if b, err := json.Marshal(resp); err == nil {
					_ = f.rediscache.SetBytes(cacheCtx, cacheKey, b, f.cacheTTL)
				}
				return resp, nil
			}
			// 没拿到锁，自旋等待缓存回填
			for i := 0; i < 5; i++ {
				time.Sleep(20 * time.Millisecond)
				if b, err := f.rediscache.GetBytes(cacheCtx, cacheKey); err == nil {
					var cached ListByFollowingResponse
					if err := json.Unmarshal(b, &cached); err == nil {
						return cached, nil
					}
				}
			}
		}
	}

	// 缓存未命中或未启用缓存，直接查 DB
	resp, err := doListByFollowingFromDB()
	if err != nil {
		return ListByFollowingResponse{}, err
	}
	// 回写缓存
	if cacheKey != "" {
		if b, err := json.Marshal(resp); err == nil {
			cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			defer cancel()
			_ = f.rediscache.SetBytes(cacheCtx, cacheKey, b, f.cacheTTL)
		}
	}
	return resp, nil
}

// ListByPopularity 热度排行。
// 先尝试 Redis 热榜（ZUNIONSTORE 合并 60 个 1 分钟窗口），降级 MySQL 三字段游标分页。
func (f *FeedService) ListByPopularity(ctx context.Context, limit int, reqAsOf int64, offset int, viewerAccountID uint, latestPopularity int64, latestBefore time.Time, latestIDBefore uint) (ListByPopularityResponse, error) {
	// 尝试 Redis 热榜路径
	if f.rediscache != nil {
		asOf := time.Now().UTC().Truncate(time.Minute)
		if reqAsOf > 0 {
			asOf = time.Unix(reqAsOf, 0).UTC().Truncate(time.Minute)
		}

		// 合并过去 60 个 1 分钟窗口的热度 ZSET
		const win = 60
		keys := make([]string, 0, win)
		for i := 0; i < win; i++ {
			keys = append(keys, "hot:video:1m:"+asOf.Add(-time.Duration(i)*time.Minute).Format("200601021504"))
		}
		dest := "hot:video:merge:1m:" + asOf.Format("200601021504")
		opCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
		defer cancel()

		exists, _ := f.rediscache.Exists(opCtx, dest)
		if !exists {
			_ = f.rediscache.ZUnionStore(opCtx, dest, keys, "SUM")
			_ = f.rediscache.Expire(opCtx, dest, 2*time.Minute)
		}

		start := int64(offset)
		stop := start + int64(limit) - 1
		members, err := f.rediscache.ZRevRange(opCtx, dest, start, stop)
		if err == nil && len(members) > 0 {
			ids := make([]uint, 0, len(members))
			for _, m := range members {
				u, err := strconv.ParseUint(m, 10, 64)
				if err == nil && u > 0 {
					ids = append(ids, uint(u))
				}
			}
			videos, err := f.repo.GetByIDs(ctx, ids)
			if err == nil {
				byID := make(map[uint]*video.Video, len(videos))
				for _, v := range videos {
					byID[v.ID] = v
				}
				ordered := make([]*video.Video, 0, len(ids))
				for _, id := range ids {
					if v := byID[id]; v != nil {
						ordered = append(ordered, v)
					}
				}
				items, err := f.buildFeedVideos(ctx, ordered, viewerAccountID)
				if err != nil {
					return ListByPopularityResponse{}, err
				}
				resp := ListByPopularityResponse{
					VideoList:  items,
					AsOf:       asOf.Unix(),
					NextOffset: offset + len(items),
					HasMore:    len(items) == limit,
				}
				if len(ordered) > 0 {
					last := ordered[len(ordered)-1]
					nextPopularity := last.Popularity
					nextBefore := last.CreateTime
					nextID := last.ID
					resp.NextLatestPopularity = &nextPopularity
					resp.NextLatestBefore = &nextBefore
					resp.NextLatestIDBefore = &nextID
				}
				return resp, nil
			}
		}
		// Redis 没数据或出错，降级到 MySQL
	}

	// MySQL 降级：三字段游标分页
	videos, err := f.repo.ListByPopularity(ctx, limit, latestPopularity, latestBefore, latestIDBefore)
	if err != nil {
		return ListByPopularityResponse{}, err
	}
	items, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
	if err != nil {
		return ListByPopularityResponse{}, err
	}
	resp := ListByPopularityResponse{
		VideoList:  items,
		AsOf:       0,
		NextOffset: 0,
		HasMore:    len(items) == limit,
	}
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextPopularity := last.Popularity
		nextBefore := last.CreateTime
		nextID := last.ID
		resp.NextLatestPopularity = &nextPopularity
		resp.NextLatestBefore = &nextBefore
		resp.NextLatestIDBefore = &nextID
	}
	return resp, nil
}

// ========== 内部辅助方法 ==========

// GetVideoByIDs 批量获取视频详情，三级缓存：L1 本地缓存 → L2 Redis → L3 MySQL。
// singleflight 防止同一个 video ID 并发穿透到 DB。
func (f *FeedService) GetVideoByIDs(ctx context.Context, videoIDs []uint) ([]*video.Video, error) {
	// TODO: 实现 L1 → L2 → L3 三级缓存查询
	if len(videoIDs) == 0 {
		return nil, nil
	}
	videoMap := make(map[uint]*video.Video)

	// 先查找本地缓存
	var missedL1 []uint
	for _, id := range videoIDs {
		cacheKey := fmt.Sprintf("video:entity:%d", id)
		if f.localcache != nil {
			if v, found := f.localcache.Get(cacheKey); found {
				if data, ok := v.(video.Video); ok {
					videoMap[id] = &data
				}
			} else {
				missedL1 = append(missedL1, id)
			}
			continue
		}
		missedL1 = append(missedL1, id)
	}
	if len(missedL1) == 0 {
		return buildOrderedResult(videoIDs, videoMap), nil
	}

	// 下面查找redis缓存
	var missedL2 []uint
	if len(missedL1) > 0 && f.rediscache != nil {
		cacheKeys := make([]string, len(missedL1))
		for i, id := range missedL1 {
			cacheKeys[i] = fmt.Sprintf("video:entity:%d", id)
		}

		cancelCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		results, err := f.rediscache.MGet(cancelCtx, cacheKeys...)
		if err != nil {
			// redis出现问题直接降解为MySql
			missedL2 = missedL1
			log.Printf("L1 Redis MGet 错误，降级到 MySQL: %v", err)
		} else {
			// redis查询成功,组装一下
			for i, result := range results {
				id := missedL1[i]
				if result != nil {
					if str, ok := result.(string); ok {
						var video video.Video
						if err := json.Unmarshal([]byte(str), &video); err == nil {
							videoMap[id] = &video
							// 写回本地缓存
							if f.localcache != nil {
								f.localcache.Set(cacheKeys[i], video, 5*time.Second)
							}
							continue
						}

					}
				}
				missedL2 = append(missedL2, id)
			}
		}

	}
	if len(missedL2) == 0 {
		return buildOrderedResult(videoIDs, videoMap), nil
	}

	// 下面查找MySql对应缓存
	// L3:MySQL
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, id := range missedL2 {
		wg.Add(1)
		go func(videoID uint) {
			defer wg.Done()
			sfKey := fmt.Sprintf("sf:entity:%d", videoID)

			v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
				videoList, err := f.repo.GetByIDs(ctx, []uint{videoID})

				if err != nil || len(videoList) == 0 {
					return nil, err
				}

				safeCopy := *videoList[0]
				cachekey := fmt.Sprintf("video:entity:%d", safeCopy.ID)
				if b, err := json.Marshal(safeCopy); err == nil {
					// 异步回写redis
					go func(k string, b []byte) {
						setCtx, setCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
						defer setCancel()

						f.rediscache.SetBytes(setCtx, k, b, time.Hour)
					}(cachekey, b)
				}
				return videoList[0], err
			})

			if err == nil && v != nil {
				safeCopy := *(v.(*video.Video))
				mu.Lock()
				videoMap[id] = &safeCopy
				mu.Unlock()
				f.localcache.Set(fmt.Sprintf("video:entity:%d", safeCopy.ID), safeCopy, 5*time.Second)
			}
		}(id)
	}
	wg.Wait()
	return buildOrderedResult(videoIDs, videoMap), nil
}

// buildFeedVideos 将原始视频列表拼装成 FeedVideoItem（含 is_liked 状态）。
func (f *FeedService) buildFeedVideos(ctx context.Context, videos []*video.Video, viewerAccountID uint) ([]FeedVideoItem, error) {
	// TODO: 批量查 is_liked → 拼装 FeedVideoItem
	feedVideos := make([]FeedVideoItem, 0, len(videos))
	videoIDs := make([]uint, len(videos))
	for i, v := range videos {
		videoIDs[i] = v.ID
	}
	likedMap, err := f.likeRepo.BatchGetLiked(ctx, videoIDs, viewerAccountID)
	if err != nil {
		return nil, err
	}
	for _, video := range videos {
		feedVideos = append(feedVideos, FeedVideoItem{
			ID:          video.ID,
			Author:      FeedAuthor{ID: video.AuthorID, Username: video.Username},
			Title:       video.Title,
			Description: video.Description,
			PlayURL:     video.PlayURL,
			CoverURL:    video.CoverURL,
			CreateTime:  video.CreateTime.Unix(),
			LikesCount:  video.LikesCount,
			IsLiked:     likedMap[video.ID],
		})
	}
	return feedVideos, nil
}

// buildOrderedResult 按照 ID 列表的顺序排列视频（map 是无序的）。
func buildOrderedResult(orderedIDs []uint, dataMap map[uint]*video.Video) []*video.Video {
	res := make([]*video.Video, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		if v, exists := dataMap[id]; exists && v != nil {
			res = append(res, v)
		}
	}
	return res
}
