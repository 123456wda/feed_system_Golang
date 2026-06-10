package feed

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
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
	return &FeedService{repo: repo, likeRepo: likeRepo, rediscache: rediscache, localcache: cache.New(3*time.Second, 5*time.Second), cacheTTL: 24 * time.Hour}
}

// ========== 四个对外查询接口 ==========

// ListLatest 最新视频流（冷热分离 + 游标分页）。
// 热数据从 Redis ZSET (feed:global_timeline) 取，不够时自动补冷尾（MySQL）。
// ZSET 为空时 singleflight 防并发重建。
func (f *FeedService) ListLatest(ctx context.Context, limit int, latestBefore time.Time, viewerAccountID uint) (ListLatestResponse, error) {
	// 获取最老的一条数据
	zsetTail, err := f.rediscache.ZRangeWithScores(ctx, "feed:global_timeline", 0, 0)
	if err != nil {
		if errors.Is(err, rediscache.ErrBreakerOpen) {
			return f.listLatestFromDB(ctx, limit, latestBefore, viewerAccountID)
		}
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
			if errors.Is(err, rediscache.ErrBreakerOpen) {
				return f.listLatestFromDB(ctx, limit, latestBefore, viewerAccountID)
			}
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

// listLatestFromDB 熔断器 Open 时的降级路径，直接查 MySQL。
func (f *FeedService) listLatestFromDB(ctx context.Context, limit int, latestBefore time.Time, viewerAccountID uint) (ListLatestResponse, error) {
	videos, err := f.repo.ListLatest(ctx, limit, latestBefore)
	if err != nil {
		return ListLatestResponse{}, err
	}
	var nextTime int64
	if len(videos) > 0 {
		nextTime = videos[len(videos)-1].CreateTime.UnixMilli()
	}
	hasMore := len(videos) == limit
	feedVideos, err := f.buildFeedVideos(ctx, videos, viewerAccountID)
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

// ListByFollowing 关注的人的视频（推拉结合模式）。
// 推路径：从 inbox:{viewerID} ZSET 取已推视频（普通用户 fanout-on-write 的结果）。
// 拉路径：从 user_videos:{bigVID} ZSET 取关注的大V最新视频（fanout-on-read）。
// 两路数据通过 k-way merge 归并排序，去重后取 top N。
// Redis 不可用时降级到原有 MySQL 子查询模式。
func (f *FeedService) ListByFollowing(ctx context.Context, limit int, latestBefore time.Time, viewerAccountID uint) (ListByFollowingResponse, error) {
	if viewerAccountID == 0 || f.rediscache == nil {
		// 未登录或 Redis 不可用，降级到 MySQL
		return f.listByFollowingFromDB(ctx, limit, latestBefore, viewerAccountID)
	}

	opCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	// 游标值：用于过滤已读过的旧数据
	maxScore := "+inf"
	if !latestBefore.IsZero() {
		maxScore = fmt.Sprintf("%d", latestBefore.UnixMilli()-1) // 防重复
	}

	// ===== Step 1 & 2: 并行读取 inbox 和大V列表 =====
	// inbox:{viewerID} 和 following:bigv:{viewerID} 两个 Redis key 互不依赖，
	// 用 goroutine 并行发起两次读取，延迟从串行 2 次 RTT 压缩为 1 次。
	inboxKey := fmt.Sprintf("inbox:%d", viewerAccountID)
	bigvKey := fmt.Sprintf("following:bigv:%d", viewerAccountID)

	// 定义通道类型，用于接收并行查询结果
	type inboxResult struct {
		items []redis.Z // inbox 中的 videoID + createTime 列表
	}
	type bigvResult struct {
		ids []string // 关注的大V ID 列表
	}

	inboxCh := make(chan inboxResult, 1)
	bigvCh := make(chan bigvResult, 1)

	// 并行发起两个 Redis 查询
	go func() {
		// 推路径：从收件箱取最近 limit*2 条（多取一些以应对后续去重/缺失）
		items, _ := f.rediscache.ZRevRangeWithScores(opCtx, inboxKey, 0, int64(limit*2)-1)
		inboxCh <- inboxResult{items: items}
	}()
	go func() {
		// 拉路径准备：获取当前用户关注的大V ID 集合
		ids, _ := f.rediscache.SMembers(opCtx, bigvKey)
		bigvCh <- bigvResult{ids: ids}
	}()

	// 等待两个并行查询完成
	inboxRes := <-inboxCh
	bigvRes := <-bigvCh

	// 解析 inbox 结果为带时间戳的视频流，同时用游标过滤已读过的旧数据
	pushStream := parseZSetWithScores(inboxRes.items, maxScore)
	bigVIDStrs := bigvRes.ids

	// ===== Step 3 & 4: 拉路径（公平采样 + 活跃度优先 + 真正的 k-way merge）=====
	var pullStreams [][]VideoWithTime
	var sortedBigVs []bigVInfo

	if len(bigVIDStrs) > 0 {
		// --- 构造 bigV ID 列表 ---
		bigVIDs := make([]uint, 0, len(bigVIDStrs))
		for _, vidStr := range bigVIDStrs {
			bigVID, err := strconv.ParseUint(vidStr, 10, 64)
			if err != nil || bigVID == 0 {
				continue
			}
			bigVIDs = append(bigVIDs, uint(bigVID))
		}

		// --- Pipeline 批量探测所有大V活跃度（每个大V取最新 1 条的时间戳）---
		// 替代旧逻辑的单个大V采样，避免以偏概全
		rdb := f.rediscache.GetRedisClient()
		sampleResults := make(map[uint]int64, len(bigVIDs))

		if rdb != nil && !f.rediscache.IsBreakerOpen() {
			pipe := rdb.Pipeline()
			sampleCmds := make(map[uint]*redis.ZSliceCmd, len(bigVIDs))
			for _, bvid := range bigVIDs {
				key := fmt.Sprintf("user_videos:%d", bvid)
				sampleCmds[bvid] = pipe.ZRevRangeWithScores(opCtx, key, 0, 0)
			}
			_, _ = pipe.Exec(opCtx)
			for bvid, cmd := range sampleCmds {
				items, err := cmd.Result()
				if err == nil && len(items) > 0 {
					sampleResults[bvid] = int64(items[0].Score)
				}
			}
		} else {
			for _, bvid := range bigVIDs {
				key := fmt.Sprintf("user_videos:%d", bvid)
				items, _ := f.rediscache.ZRevRangeWithScores(opCtx, key, 0, 0)
				if len(items) > 0 {
					sampleResults[bvid] = int64(items[0].Score)
				}
			}
		}

		// --- 公平提前终止：取所有大V中最新的时间戳 ---
		newestBigVScore := int64(0)
		for _, score := range sampleResults {
			if score > newestBigVScore {
				newestBigVScore = score
			}
		}

		shouldSkipPull := false
		if len(pushStream) >= limit {
			inboxOldest := pushStream[len(pushStream)-1].CreateTime
			if newestBigVScore <= inboxOldest {
				shouldSkipPull = true
			}
		}

		if !shouldSkipPull && len(sampleResults) > 0 {
			// --- 按活跃度降序排序大V ---
			sortedBigVs = make([]bigVInfo, 0, len(sampleResults))
			for bvid, score := range sampleResults {
				sortedBigVs = append(sortedBigVs, bigVInfo{id: bvid, newestAt: score})
			}
			sort.Slice(sortedBigVs, func(i, j int) bool {
				return sortedBigVs[i].newestAt > sortedBigVs[j].newestAt
			})

			// --- 按活跃度优先分配拉取配额 ---
			needed := limit - len(pushStream)
			if needed <= 0 {
				needed = limit // 安全兜底
			}

			if rdb != nil {
				pipe := rdb.Pipeline()
				pullCmds := make(map[uint]*redis.ZSliceCmd, len(sortedBigVs))
				remaining := needed
				for _, bv := range sortedBigVs {
					if remaining <= 0 {
						break
					}
					// 跳过：该大V最新内容比 inbox 最老还旧，拉出来也不会排到前面
					if len(pushStream) > 0 && bv.newestAt <= pushStream[len(pushStream)-1].CreateTime {
						continue
					}
					take := remaining
					if take > 10 {
						take = 10
					}
					key := fmt.Sprintf("user_videos:%d", bv.id)
					pullCmds[bv.id] = pipe.ZRevRangeWithScores(opCtx, key, 0, int64(take)-1)
					remaining -= take
				}
				_, _ = pipe.Exec(opCtx)

				for _, cmd := range pullCmds {
					items, err := cmd.Result()
					if err != nil || len(items) == 0 {
						continue
					}
					pullStreams = append(pullStreams, parseZSetWithScores(items, maxScore))
				}
			} else {
				remaining := needed
				for _, bv := range sortedBigVs {
					if remaining <= 0 {
						break
					}
					// 跳过：该大V最新内容比 inbox 最老还旧
					if len(pushStream) > 0 && bv.newestAt <= pushStream[len(pushStream)-1].CreateTime {
						continue
					}
					take := remaining
					if take > 10 {
						take = 10
					}
					key := fmt.Sprintf("user_videos:%d", bv.id)
					items, _ := f.rediscache.ZRevRangeWithScores(opCtx, key, 0, int64(take)-1)
					if len(items) > 0 {
						pullStreams = append(pullStreams, parseZSetWithScores(items, maxScore))
					}
					remaining -= take
				}
			}
		}
	}

	// ===== 真正的 k-way merge（堆大小 = 流数量 K，O(N log K)）=====
	cursors := make([]*streamCursor, 0, 1+len(pullStreams))
	cursors = append(cursors, &streamCursor{items: pushStream, source: "inbox"})
	for i, ps := range pullStreams {
		source := fmt.Sprintf("bigv:%d", i)
		if i < len(sortedBigVs) {
			source = fmt.Sprintf("bigv:%d", sortedBigVs[i].id)
		}
		cursors = append(cursors, &streamCursor{items: ps, source: source})
	}

	mergedIDs := mergeAndDedup(cursors, limit)

	if len(mergedIDs) == 0 {
		// Redis 路径无数据，降级到 MySQL
		return f.listByFollowingFromDB(ctx, limit, latestBefore, viewerAccountID)
	}

	// 通过三级缓存获取完整视频信息
	baseVideos, err := f.GetVideoByIDs(ctx, mergedIDs)
	if err != nil {
		return f.listByFollowingFromDB(ctx, limit, latestBefore, viewerAccountID)
	}

	// 按 createTime 降序排序（GetVideoByIDs 保持的是 mergedIDs 的顺序，但可能有缺失）
	sort.Slice(baseVideos, func(i, j int) bool {
		return baseVideos[i].CreateTime.After(baseVideos[j].CreateTime)
	})

	var nextTime int64
	if len(baseVideos) > 0 {
		nextTime = baseVideos[len(baseVideos)-1].CreateTime.UnixMilli()
	}
	hasMore := len(baseVideos) >= limit

	feedVideos, err := f.buildFeedVideos(ctx, baseVideos, viewerAccountID)
	if err != nil {
		return ListByFollowingResponse{}, err
	}

	return ListByFollowingResponse{
		VideoList: feedVideos,
		NextTime:  nextTime,
		HasMore:   hasMore,
	}, nil
}

// listByFollowingFromDB 降级路径：MySQL 子查询（保留原有逻辑）。
func (f *FeedService) listByFollowingFromDB(ctx context.Context, limit int, latestBefore time.Time, viewerAccountID uint) (ListByFollowingResponse, error) {
	videos, err := f.repo.ListByFollowing(ctx, limit, viewerAccountID, latestBefore)
	if err != nil {
		return ListByFollowingResponse{}, err
	}
	var nextTime int64
	if len(videos) > 0 {
		nextTime = videos[len(videos)-1].CreateTime.UnixMilli()
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

// ========== 推拉结合辅助类型和方法 ==========

// VideoWithTime 用于归并排序的中间结构，包含 videoID 和创建时间毫秒时间戳。
type VideoWithTime struct {
	VideoID    uint
	CreateTime int64
}

// parseZSetWithScores 将 ZSET 的带分数结果解析为 VideoWithTime 切片。
// 保留 score（createTime 毫秒时间戳）用于归并排序。
// maxScore 为游标过滤上限，score >= maxScore 的条目会被过滤掉。
func parseZSetWithScores(items []redis.Z, maxScore string) []VideoWithTime {
	var maxScoreVal int64
	if maxScore != "+inf" {
		maxScoreVal, _ = strconv.ParseInt(maxScore, 10, 64)
	} else {
		maxScoreVal = 1<<63 - 1
	}

	result := make([]VideoWithTime, 0, len(items))
	for _, item := range items {
		id, err := strconv.ParseUint(fmt.Sprintf("%v", item.Member), 10, 64)
		if err != nil || id == 0 {
			continue
		}
		score := int64(item.Score)
		// 过滤掉 >= maxScore 的条目（游标分页防重复）
		if score >= maxScoreVal {
			continue
		}
		result = append(result, VideoWithTime{
			VideoID:    uint(id),
			CreateTime: score,
		})
	}
	return result
}

// streamCursor 已排序视频流的迭代器，用于真正的 k-way merge。
type streamCursor struct {
	items  []VideoWithTime
	pos    int
	source string // 来源标识，调试用
}

func (c *streamCursor) peek() (VideoWithTime, bool) {
	if c.pos >= len(c.items) {
		return VideoWithTime{}, false
	}
	return c.items[c.pos], true
}

func (c *streamCursor) advance() {
	c.pos++
}

// mergeItem 堆中的元素，携带来源流引用，pop 后可从同一流取下一个入堆。
type mergeItem struct {
	video  VideoWithTime
	cursor *streamCursor
}

// mergeHeap 按 CreateTime 降序的最大堆，堆大小 = 流数量 K。
type mergeHeap []mergeItem

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return h[i].video.CreateTime > h[j].video.CreateTime }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)        { *h = append(*h, x.(mergeItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// bigVInfo 大V活跃度信息，用于按 newestAt 排序分配拉取配额。
type bigVInfo struct {
	id       uint
	newestAt int64 // 最新内容的 createTime 毫秒时间戳
}

// mergeAndDedup 真正的 k-way merge：堆大小 = 流数量 K，每次 pop 后从该流 advance 取下一个入堆。
// 时间复杂度 O(N log K)，空间复杂度 O(K)。
func mergeAndDedup(cursors []*streamCursor, limit int) []uint {
	h := &mergeHeap{}
	heap.Init(h)

	// 每个流入堆第一个元素
	for _, cur := range cursors {
		if item, ok := cur.peek(); ok {
			cur.advance()
			heap.Push(h, mergeItem{video: item, cursor: cur})
		}
	}

	seen := make(map[uint]bool, limit)
	result := make([]uint, 0, limit)

	for h.Len() > 0 && len(result) < limit {
		top := heap.Pop(h).(mergeItem)
		if !seen[top.video.VideoID] {
			seen[top.video.VideoID] = true
			result = append(result, top.video.VideoID)
		}
		// 从同一个流取下一个元素入堆
		if next, ok := top.cursor.peek(); ok {
			top.cursor.advance()
			top.video = next
			heap.Push(h, top)
		}
	}
	return result
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

		// 合并过去 60 个 1 分钟窗口的热度 ZSET，越老的窗口权重越小
		const win = 60
		const decay = 0.95 // 每分钟衰减 5%
		keys := make([]string, 0, win)
		weights := make([]float64, 0, win)
		for i := 0; i < win; i++ {
			keys = append(keys, "hot:video:1m:"+asOf.Add(-time.Duration(i)*time.Minute).Format("200601021504"))
			weights = append(weights, math.Pow(decay, float64(i)))
		}
		dest := "hot:video:merge:1m:" + asOf.Format("200601021504")
		opCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
		defer cancel()

		exists, _ := f.rediscache.Exists(opCtx, dest)
		if !exists {
			_ = f.rediscache.ZUnionStoreWithWeights(opCtx, dest, keys, weights, "SUM")
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
	if len(videoIDs) == 0 {
		return nil, nil
	}
	videoMap := make(map[uint]*video.Video)

	// 先查找本地缓存
	var missedL1 []uint
	for _, id := range videoIDs {
		cacheKey := fmt.Sprintf("video:detail:id=%d", id)
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
			cacheKeys[i] = fmt.Sprintf("video:detail:id=%d", id)
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
				cachekey := fmt.Sprintf("video:detail:id=%d", safeCopy.ID)
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
				f.localcache.Set(fmt.Sprintf("video:detail:id=%d", safeCopy.ID), safeCopy, 5*time.Second)
			}
		}(id)
	}
	wg.Wait()
	return buildOrderedResult(videoIDs, videoMap), nil
}

// buildFeedVideos 将原始视频列表拼装成 FeedVideoItem（含 is_liked 状态）。
func (f *FeedService) buildFeedVideos(ctx context.Context, videos []*video.Video, viewerAccountID uint) ([]FeedVideoItem, error) {
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
