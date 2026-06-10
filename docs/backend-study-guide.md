# Go 后端短视频 Feed 系统 -- 技术学习文档

> 项目技术栈：Go + Gin + GORM + Redis + RabbitMQ + MySQL + JWT
>
> 本文档基于项目实际代码编写，所有代码引用均指向 `backend_repeat/` 目录下的真实文件和函数。

---

## 第1章：项目架构总览

### 1.1 系统架构

本项目采用**双进程架构**，将在线请求处理与异步事件消费分离：

| 进程 | 入口 | 职责 |
|------|------|------|
| API 进程 | `backend_repeat/cmd/main.go:main` | 接收 HTTP 请求、参数校验、JWT 认证、MQ 投递、返回响应 |
| Worker 进程 | `backend_repeat/cmd/worker/main.go:main` | 消费 MQ 消息、持久化到 MySQL、更新 Redis 缓存、推拉结合 fanout |

外部依赖：

- **MySQL**：核心持久化存储，存储用户、视频、点赞、评论、关注关系、outbox 消息
- **Redis**：分布式缓存（视频实体、关注 feed）、ZSET 时间线/热榜、分布式锁、限流计数器、Token 缓存
- **RabbitMQ**：异步事件总线，6 个 Topic Exchange 承载点赞/评论/关注/热度/时间线/fanout 事件

### 1.2 分层架构

项目严格遵循 **Handler -> Service -> Repository** 三层架构，以四个业务模块为例：

```
account 模块:  AccountHandler -> AccountService -> AccountRepository
video 模块:    VideoHandler   -> VideoService   -> VideoRepository
social 模块:   SocialHandler  -> SocialService  -> SocialRepository
feed 模块:     FeedHandler    -> FeedService    -> FeedRepository
```

每一层的职责边界：

- **Handler**（`backend_repeat/internal/account/handler.go` 等）：解析 HTTP 请求参数、调用 Service、组装响应。通过 `binding:"required"` 做基础参数校验。
- **Service**（`backend_repeat/internal/account/service.go` 等）：核心业务逻辑，编排 Repository 调用、MQ 投递、缓存操作。Service 不直接访问 `repo.db`，事务通过 `Transaction` 方法获取 `*gorm.DB` 后调用 repo 的 `InTx` 方法。
- **Repository**（`backend_repeat/internal/account/repo.go` 等）：纯数据访问层，封装 GORM 查询。提供事务版本方法（如 `LikeInTx`）和普通版本方法。

### 1.3 请求流转全链路 -- 以点赞为例

完整的点赞请求链路，从用户点击到数据持久化：

```
用户点击点赞按钮
    |
    v
[API 进程] POST /like/like
    |
    v
LikeHandler.Like (backend_repeat/internal/video/like_handler.go)
    |-- 解析请求参数 (accountID, videoID)
    |-- 从 Gin context 获取 accountID (JWT 中间件写入)
    |
    v
LikeService.Like (backend_repeat/internal/video/like_service.go:Like)
    |-- 1. 参数校验 (accountID == 0 || videoID == 0)
    |-- 2. 确认视频存在 (videoRepo.IsExist)
    |-- 3. 构造 Like 结构体
    |
    |-- [MQ 优先路径]
    |   |-- likeMQ.Like(ctx, accountID, videoID)  --> 投递到 like.events exchange
    |   |-- popularityMQ.Update(ctx, videoID, 1)   --> 投递到 video.popularity.events exchange
    |   |-- 两条 MQ 都成功 --> 直接返回 (异步路径)
    |
    |-- [降级路径] (任一 MQ 投递失败)
    |   |-- LikeMQ 失败: 同步 MySQL 事务
    |   |   |-- likeRepo.LikeInTx (插入点赞记录)
    |   |   |-- videoRepo.ChangeLikesCount (GREATEST 兜底)
    |   |   |-- videoRepo.ChangePopularity (GREATEST 兜底)
    |   |-- PopularityMQ 失败: 直接调用 UpdatePopularityCache
    |
    v
[Worker 进程] LikeWorker.Run (backend_repeat/internal/worker/likeworker.go)
    |-- 从 like.events 队列消费消息
    |-- handleDelivery -> process -> applyLike
    |   |-- videoRepo.IsExist (确认视频存在)
    |   |-- likeRepo.Transaction (开启事务)
    |   |   |-- likeRepo.LikeIgnoreDuplicateInTx (事务内幂等插入，catch 1062)
    |   |   |-- created == true --> videoRepo.ChangeLikesCount + videoRepo.ChangePopularity (同一个 tx)
    |   |   |-- created == false --> 跳过 (重复消息，幂等，事务空提交)
    |-- Ack(false) 确认消费
```

### 1.4 模块依赖关系

```
cmd/main.go (API 进程)
    |-- config.LoadLocalDev        加载配置
    |-- db.NewDB + AutoMigrate     初始化 MySQL
    |-- rediscache.NewFromEnv      初始化 Redis
    |-- rabbitmq.NewRabbitMQ       初始化 RabbitMQ
    |-- observability.NewPprofServer 启动 pprof
    |-- http.SetRouter             组装所有路由、依赖注入
    |-- http.StartServer           异步启动 HTTP 服务
    |-- http.GracefulShutdown      优雅停机

cmd/worker/main.go (Worker 进程)
    |-- config.LoadLocalDev
    |-- db.NewDB                   初始化 MySQL (不做 AutoMigrate)
    |-- rediscache.NewFromEnv      初始化 Redis
    |-- rabbitmq.NewRabbitMQ
    |-- declareXxxTopology         声明所有 MQ 拓扑 (Exchange + Queue + Binding)
    |-- declareFanoutTopology      声明 fanout 拓扑 (复用 timeline exchange)
    |-- rbq.Ch.Qos(prefetchCount=50) 设置预取数量
    |-- NewSocialWorker / NewLikeWorker / NewCommentWorker / NewPopularityWorker / NewFanoutWorker
    |-- signal.NotifyContext       监听中断信号
    |-- observability.NewPprofServer
    |-- errCh 收集 5 个 goroutine 的错误
```

### 1.5 MySQL 表结构与索引设计

项目使用 GORM AutoMigrate 自动建表，共 6 张表。索引设计是幂等消费和查询性能的基础。

**Account 表**

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| id | uint | PK, AUTO_INCREMENT | 用户 ID |
| username | string | UNIQUE | 用户名 |
| password | string | — | bcrypt 哈希（cost=10） |
| token | string | — | 当前有效 JWT（登出/改名时清空） |
| follower_count | int64 | — | 粉丝计数（冗余字段，避免 COUNT 查询） |

**Video 表**

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| id | uint | PK | 视频 ID |
| author_id | uint | INDEX | 作者 ID（按作者查视频列表） |
| username | string | — | 作者名（冗余，避免 JOIN） |
| title | string | — | 标题 |
| description | string | — | 描述 |
| play_url | string | — | 视频文件路径 |
| cover_url | string | — | 封面文件路径 |
| create_time | time.Time | AUTO | 创建时间（Feed 排序依据） |
| likes_count | int64 | — | 点赞数（`GREATEST(likes_count + delta, 0)` 保底） |
| popularity | int64 | — | 热度值（`GREATEST(popularity + delta, 0)` 保底） |

**Like 表** — 支撑幂等消费的核心

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| id | uint | PK | — |
| video_id | uint | **UNIQUE INDEX (video_id, account_id)** | 联合唯一索引，保证同一用户不能重复点赞 |
| account_id | uint | ↑ 同上 | Worker 消费时 catch 1062 实现幂等 |
| created_at | time.Time | AUTO | — |

**Comment 表**

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| id | uint | PK | — |
| username | string | INDEX | 评论者名 |
| video_id | uint | INDEX | 所属视频（评论列表查询） |
| author_id | uint | INDEX | 评论者 ID（仅作者可删） |
| content | string (text) | — | 评论内容 |
| created_at | time.Time | AUTO | — |

**Social 表** — 支撑幂等消费

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| id | uint | PK | — |
| follower_id | uint | **UNIQUE INDEX (follower_id, vlogger_id)** | 联合唯一索引，保证不能重复关注 |
| vlogger_id | uint | ↑ 同上 | Worker 消费时 catch 1062 实现幂等 |

**OutboxMsg 表** — Outbox 模式本地消息表

| 字段 | 类型 | 约束 | 说明 |
|------|------|------|------|
| id | uint | PK | — |
| video_id | uint | INDEX | 关联视频 |
| author_id | uint | INDEX | 作者 ID（FanoutMQ 需要） |
| event_type | string | — | 事件类型（`"video_published"`） |
| create_time | time.Time | — | 视频创建时间（TimelineMQ score） |
| status | string | INDEX | `"pending"` / `"done"`，轮询器按 status 筛选 |

**索引设计要点**：
- Like 和 Social 的联合唯一索引是幂等消费的基石：Worker 收到重复消息时，INSERT 触发 1062 错误，事务内静默忽略
- Video.author_id 索引支撑"按作者查视频列表"高频查询
- Comment.video_id 索引支撑"按视频查评论列表"高频查询
- OutboxMsg.status 索引支撑轮询器的 `WHERE status = 'pending'` 查询

### 自检题

1. API 进程和 Worker 进程各自连接了哪些外部依赖？为什么 Worker 不需要 AutoMigrate？
2. Handler 层的 `binding:"required"` 和 Service 层的手动校验分别负责什么？两者的校验粒度有何不同？
3. 点赞的 MQ 优先路径和降级路径在数据一致性上有什么区别？降级路径的延迟开销大约是多少？
4. 为什么 `VideoRepository.ChangeLikesCount` 需要接受 `tx *gorm.DB` 参数？如果不传 tx 会有什么问题？
5. Worker 进程的 `prefetchCount` 设为 50 有什么作用？设太大或太小分别有什么后果？
6. `signal.NotifyContext` 和 `signal.Notify` 在优雅停机中的区别是什么？为什么 Worker 用前者而 API 用后者？
7. API 进程中 `SetRouter` 函数承担了哪些职责？为什么 MQ 拓扑声明在 Worker 进程而不是 API 进程？
8. 如果 Redis 或 RabbitMQ 连接失败，API 进程会怎样处理？这种 fail-open 设计有什么优缺点？
9. FanoutWorker 的启动条件是什么？为什么依赖 Redis？如果 Redis 不可用，fanout 会怎样？
10. OutboxPoller 的双投递（TimelineMQ + FanoutMQ）顺序是什么？为什么要先 TimelineMQ？如果反过来会有什么问题？
11. 为什么项目选择双进程架构而不是单进程内 goroutine 消费？从部署、扩缩容、故障隔离三个角度分析。
12. 如果 Worker 进程 crash 了，正在处理的消息会怎样？RabbitMQ 的 redelivery 机制如何保证不丢？
13. Like 表和 Social 表的联合唯一索引除了支撑幂等消费，还有什么作用？如果没有这个索引会怎样？
14. OutboxMsg 表的 status 字段为什么需要索引？轮询器的查询模式是什么？

---

## 第2章：Redis 深度应用

### 2.1 三级缓存架构

本项目的视频详情查询采用三级缓存架构，核心实现在 `backend_repeat/internal/feed/service.go:GetVideoByIDs`。

**缓存层级：**

| 层级 | 存储 | TTL | 作用 |
|------|------|-----|------|
| L1 | go-cache 本地缓存 (`github.com/patrickmn/go-cache`) | 3-5 秒（默认 3s，回写 5s） | 进程内缓存，零网络开销，QPS 极高 |
| L2 | Redis 分布式缓存 | 1 小时 | 跨实例共享，MGet 批量查询 |
| L3 | MySQL 源数据 | 持久化 | 数据最终来源 |

**查询流程：**

```go
// backend_repeat/internal/feed/service.go:GetVideoByIDs
func (f *FeedService) GetVideoByIDs(ctx context.Context, videoIDs []uint) ([]*video.Video, error) {
    videoMap := make(map[uint]*video.Video)

    // L1: 本地缓存逐个查找
    var missedL1 []uint
    for _, id := range videoIDs {
        cacheKey := fmt.Sprintf("video:detail:id=%d", id)
        if v, found := f.localcache.Get(cacheKey); found {
            // 命中 L1，直接使用
            videoMap[id] = &data
        } else {
            missedL1 = append(missedL1, id)
        }
    }

    // L2: Redis MGet 批量查询 L1 未命中的 key
    if len(missedL1) > 0 && f.rediscache != nil {
        cacheKeys := make([]string, len(missedL1))
        for i, id := range missedL1 {
            cacheKeys[i] = fmt.Sprintf("video:detail:id=%d", id)
        }
        results, err := f.rediscache.MGet(cancelCtx, cacheKeys...)
        // 解析结果，命中的写回 L1 (回写 5s TTL)
        // 未命中的收集到 missedL2
    }

    // L3: MySQL 查询 + singleflight 防穿透
    for _, id := range missedL2 {
        wg.Add(1)
        go func(videoID uint) {
            sfKey := fmt.Sprintf("sf:entity:%d", videoID)
            v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
                videoList, err := f.repo.GetByIDs(ctx, []uint{videoID})
                // 异步回写 Redis (go routine, 1h TTL)
                go func() { f.rediscache.SetBytes(setCtx, k, b, time.Hour) }()
                return videoList[0], err
            })
            // 写回 L1 本地缓存 (回写 5s TTL)
            f.localcache.Set(cacheKey, safeCopy, 5*time.Second)
        }(id)
    }
    wg.Wait()
}
```

**关键设计点：**

1. **singleflight 防击穿**：同一个 `videoID` 的并发请求通过 `f.requestGroup.Do(sfKey, ...)` 合并为一次 DB 查询。`shared` 返回值为 `true` 表示本次结果是从其他请求"搭便车"获得的。

2. **异步回写**：MySQL 查询成功后，通过 `go func()` 异步回写 Redis，不阻塞当前请求。L1 回写是同步的（因为是进程内操作，开销极小）。

3. **MGet 批量查询**：L2 层使用 `MGet` 一次网络往返获取多个 key，而非逐个 `Get`，大幅减少 Redis 网络开销。

4. **Redis 异常降级**：L2 查询失败时，直接将 `missedL1` 整体降级到 L3（MySQL），保证可用性。

### 2.2 分布式锁

分布式锁实现在 `backend_repeat/internal/middleware/redis:Lock` 和 `UnLock`。

**加锁 -- `Lock` 方法：**

```go
// backend_repeat/internal/middleware/redis/redis.go:Lock
func (c *Client) Lock(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
    token, err := randToken(16)  // 生成随机 token 作为锁持有者标识
    ok, err := c.rdb.SetNX(ctx, key, token, ttl).Result()  // SET key token NX PX ttl
    return token, ok, nil
}
```

- `SetNX`（SET if Not eXists）保证原子性：只有一个客户端能成功设置
- `token` 是随机 16 字节 hex 字符串，用于标识锁的持有者
- `ttl` 自动过期，防止死锁

**释放锁 -- `UnLock` 方法（Lua 脚本）：**

```go
// backend_repeat/internal/middleware/redis/redis.go:unlockScript
var unlockScript = redis.NewScript(`
    local token = redis.call('GET',KEYS[1])
    if token==ARGV[1] then
        return redis.call('DEL',KEYS[1])
    else
        return 0
    end
`)
```

**为什么必须用 Lua 脚本？**

如果用两步操作（GET + DEL），存在以下竞态：

```
时间线    客户端A              客户端B
T1        GET key -> tokenA
T2        (A 处理超时，锁自动过期)
T3                              Lock(key) -> tokenB (获得锁)
T4        DEL key
T5                              (B 的锁被 A 误删了!)
```

Lua 脚本在 Redis 中原子执行 GET + 比较 + DEL，保证只有持有正确 token 的客户端才能释放锁。

**应用场景 -- `ListByFollowing`：**

`ListByFollowing` 本身不使用分布式锁或 singleflight。其 Redis 不可用时降级到 MySQL 子查询（`listByFollowingFromDB`）；下游 `GetVideoByIDs` 使用 singleflight 防击穿。分布式锁在项目中作为通用工具存在，供需要跨实例保护的场景使用。

### 2.3 限流 Lua 脚本

项目使用**双策略限流**：固定窗口（登录/注册）和滑动窗口（点赞/评论/关注）。本节介绍固定窗口实现，滑动窗口详见 §7.3。

固定窗口限流实现在 `backend_repeat/internal/middleware/redis/redis.go:IncrementWithExpire`。

```go
// backend_repeat/internal/middleware/redis/redis.go:incrementWithExpireScript
var incrementWithExpireScript = redis.NewScript(`
    local count = redis.call('INCR',KEYS[1])
    if count==1 then
        redis.call('PEXPIRE',KEYS[1],ARGV[1])
    end
    return count
`)
```

**核心语义：**

- 首次 INCR（count == 1）：设置 PEXPIRE，启动固定窗口
- 后续 INCR（count > 1）：只累加计数，**不续期**（保证窗口不会被无限延长）
- 窗口到期后 key 自动删除，计数归零，新的请求开始新窗口

**为什么只在 count==1 时设置 PEXPIRE？**

如果每次 INCR 都重置 PEXPIRE，那么持续不断的请求会让 key 永不过期，限流窗口被无限延长，失去了"固定窗口"的语义。只在首次设置，保证窗口从第一个请求开始计时，到期后自动过期。

**测试验证：**

```go
// backend_repeat/internal/middleware/redis/redis_test.go:TestIncrementWithExpireSetsTTLWithoutExtendingWindow
// 关键验证点：第二次 INCR 后，TTL 不应被重置
ttlAfterSecond := mr.TTL(key)
if ttlAfterSecond != ttlBeforeSecond {
    t.Fatalf("expected ttl to stay at %s, got %s", ttlBeforeSecond, ttlAfterSecond)
}
```

### 2.4 ZSET 三场景

#### 场景1：全局时间线 `feed:global_timeline`

```go
// backend_repeat/internal/worker/outboxworker.go:StartConsumer
timelineKey := "feed:global_timeline"
redisClient.ZAdd(ctx, timelineKey, redis.Z{
    Member: fmt.Sprintf("%d", val.VideoID),
    Score:  float64(val.CreateTime),  // 视频创建时间毫秒时间戳
})
// 裁剪，只保留最新 1000 条
redisClient.ZRemRangeByRank(ctx, timelineKey, 0, -1001)
```

- **Score**：视频创建时间的毫秒时间戳
- **查询**：`ZRevRangeByScore` 按分数从高到低分页取数据
- **裁剪**：`ZRemRangeByRank(0, -1001)` 移除排名 0 到 -1001 的成员，只保留最新 1000 条
- **用途**：`ListLatest` 热数据查询

#### 场景2：热度窗口 `hot:video:1m:YYYYMMDDHHmm`

```go
// backend_repeat/internal/video/popularity_cache.go:UpdatePopularityCache
now := time.Now().UTC().Truncate(time.Minute)
windowKey := "hot:video:1m:" + now.Format("200601021504")
_ = cache.ZincrBy(opCtx, windowKey, member, float64(change))
_ = cache.Expire(opCtx, windowKey, 2*time.Hour)  // 2 小时自动过期
```

- **Key 格式**：`hot:video:1m:202605061435`（年月日时分）
- **Score**：点赞/评论次数的累计值（通过 ZINCRBY 增量更新）
- **过期**：2 小时自动过期，防止内存泄漏
- **触发时机**：每次点赞/取消点赞/评论时调用 `UpdatePopularityCache`

#### 场景3：合并热榜 `hot:video:merge:1m:*`

```go
// backend_repeat/internal/feed/service.go:ListByPopularity
const win = 60
const decay = 0.95 // 每分钟衰减 5%
keys := make([]string, 0, win)
weights := make([]float64, 0, win)
for i := 0; i < win; i++ {
    keys = append(keys, "hot:video:1m:"+asOf.Add(-time.Duration(i)*time.Minute).Format("200601021504"))
    weights = append(weights, math.Pow(decay, float64(i)))
}
dest := "hot:video:merge:1m:" + asOf.Format("200601021504")

exists, _ := f.rediscache.Exists(opCtx, dest)
if !exists {
    _ = f.rediscache.ZUnionStoreWithWeights(opCtx, dest, keys, weights, "SUM")
    _ = f.rediscache.Expire(opCtx, dest, 2*time.Minute)  // 2 分钟 TTL 缓存结果
}
```

- **合并逻辑**：ZUNIONSTORE 将过去 60 个 1 分钟窗口按时间衰减加权合并（decay=0.95，最新窗口权重 1.0，59 分钟前权重 0.046）
- **缓存**：合并结果缓存 2 分钟，避免每次都做 ZUNIONSTORE
- **分页**：ZRevRange 按排名从高到低取数据

### 2.5 缓存击穿防护机制

本项目在 `GetVideoByIDs` 和 `ListLatest` 中通过 singleflight 防止缓存击穿，共 4 处调用，本质都是缓存没命中需要回源 DB 时防止并发请求重复打数据库：

**核心机制：singleflight**

```go
// backend_repeat/internal/feed/service.go:GetVideoByIDs
sfKey := fmt.Sprintf("sf:entity:%d", videoID)
v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
    // 同一个 videoID 的并发请求只会执行一次这个函数
    return f.repo.GetByIDs(ctx, []uint{videoID})
})
```

`GetVideoByIDs` 的 L3 回源路径使用 singleflight 合并同 key 并发请求，同一进程内同一 videoID 只会发起一次 DB 查询，其余请求等待并共享结果。

```go
// backend_repeat/internal/feed/service.go:ListLatest（冷查询路径）
sfKey := fmt.Sprintf("sf:cold:listLatest:%d:%d", limit, reqTime)
v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
    return f.repo.ListLatest(ctx, limit, latestBefore)
})

// backend_repeat/internal/feed/service.go:ListLatest（冷热衔接补尾路径）
sfKey := fmt.Sprintf("sf:stitch:listLatest:%d:%d", remainlimit, OldMax.UnixMilli())
v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
    return f.repo.ListLatest(ctx, remainlimit, OldMax)
})
```

`ListLatest` 有 3 处 singleflight：ZSET 为空时防并发重建（`sf:fallback:feed:global_timeline_rebuild`）、冷查询路径（`sf:cold:listLatest:{limit}:{reqTime}`）、热数据不够时的冷热衔接补尾（`sf:stitch:listLatest:{remain}:{cursor}`）。三者本质一样——缓存没命中需要回源 DB，singleflight 合并并发请求。

**VideoService.GetDetail 用分布式锁防击穿：**

```go
// backend_repeat/internal/video/video_service.go:GetDetail
// Redis 缓存 miss 时，用分布式锁保护
lockKey := "lock:" + cacheKey
token, ok, err := s.cache.Lock(lockCtx, lockKey, 2*time.Second)
if err == nil && ok {
    defer s.cache.UnLock(context.Background(), lockKey, token)
    // 拿到锁：查 DB → 回写 Redis → 返回
    video, err := s.repo.GetDetail(ctx, id)
    s.cache.SetBytes(cacheCtx, cacheKey, b, s.cacheTTL)
    return video, nil
}
// 没拿到锁：轮询等待别人回填缓存
for i := 0; i < 5; i++ {
    time.After(20 * time.Millisecond)
    if v, err := GetCache(); err == nil {
        return v, nil
    }
}
```

单机部署下，singleflight 和分布式锁效果一样。项目中两种都用是为了丰富技术栈。`ListByFollowing` 本身不使用 singleflight，Redis 不可用时降级到 MySQL 子查询（`listByFollowingFromDB`）；下游 `GetVideoByIDs` 使用 singleflight 防击穿。

### 2.6 缓存一致性策略

本项目针对不同缓存场景采用不同的一致性策略：

**策略一：异步回写 + TTL（视频实体缓存）**

```go
// backend_repeat/internal/feed/service.go:GetVideoByIDs
// MySQL 查询成功后，go routine 异步回写 Redis
go func(k string, b []byte) {
    setCtx, setCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
    defer setCancel()
    f.rediscache.SetBytes(setCtx, k, b, time.Hour)  // 1h TTL
}(cachekey, b)
```

- 视频实体变化频率低，TTL 1 小时足够
- 异步回写不阻塞请求
- TTL 到期后自然淘汰，保证最终一致性

**策略二：事件驱动失效（热度缓存）**

```go
// backend_repeat/internal/video/popularity_cache.go:UpdatePopularityCache
// 每次点赞/评论时，主动删除详情缓存
_ = cache.Del(context.Background(), fmt.Sprintf("video:detail:id=%d", videoID))
// 同时更新热度窗口 ZSET
_ = cache.ZincrBy(opCtx, windowKey, member, float64(change))
```

- 点赞/评论后立即删除详情缓存，下次读取时从 DB 获取最新数据
- 热度窗口通过 ZINCRBY 实时更新，保证热榜数据近实时

**策略三：Outbox 模式（时间线）**

```go
// backend_repeat/internal/video/video_repo.go:PublishVideo
err := r.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
    if err := db.Create(video).Error; err != nil {
        return err
    }
    msg := OutboxMsg{
        VideoID:    video.ID,
        EventType:  "video_published",
        Status:     "pending",
        CreateTime: video.CreateTime,
    }
    if err := db.Create(&msg).Error; err != nil {
        return err
    }
    return nil
})
```

- 视频入库和 outbox 消息写入在同一个 MySQL 事务中
- OutboxPoller 轮询 pending 消息投递到 MQ
- 消费者收到后 ZADD 到时间线 ZSET
- 保证"视频写入 DB"和"事件投递到 MQ"的最终一致性

### 2.7 singleflight vs 分布式锁的选择

| 维度 | singleflight | 分布式锁 |
|------|-------------|---------|
| 作用范围 | 单实例内去重 | 跨实例保护 |
| 适用场景 | 全局 key（如 `GetVideoByIDs` 的 video entity、`ListLatest` 冷查询） | 需要跨实例协调的场景（如缓存重建的 leader election） |
| 实现 | `sync/singleflight.Group.Do` | Redis SETNX + Lua DEL |
| 开销 | 零网络开销（进程内） | 一次 Redis 网络往返 |
| 局限 | 只在单个进程内有效 | 需要处理锁超时、续期等问题 |

**本项目的选择：**

项目中两种都用了——singleflight 在 FeedService（4 处），分布式锁在 VideoService.GetDetail。单机部署下两者效果一样，用两种技术是为了丰富技术栈，不是基于场景差异的选型。

- `GetVideoByIDs` / `ListLatest`：singleflight `sf:entity:{id}`、`sf:fallback:...`、`sf:cold:...`、`sf:stitch:...`
- `ListByFollowing`：MySQL 降级兜底（`listByFollowingFromDB`），下游 `GetVideoByIDs` 使用 singleflight
- `GetDetail`：分布式锁（Lock + 轮询等待）

### 2.8 推拉结合的 Redis 架构

本项目实现了**推拉结合**（Push-Pull Hybrid）Feed 流模型，核心思想是：普通用户（粉丝 < 10000）发布视频时 fanout-on-write 推送到粉丝收件箱，大 V（粉丝 >= 10000）不推送，读取时 fanout-on-read 从发件箱拉取，两路数据归并排序。

**Redis Key 设计：**

| Key | 类型 | TTL | 用途 |
|-----|------|-----|------|
| `inbox:{userID}` | ZSET | 无(cap 控制) | 用户收件箱，score=createTime 毫秒，cap 500 |
| `user_videos:{authorID}` | ZSET | 24h | 作者发件箱，score=createTime 毫秒，cap 50 |
| `following:bigv:{followerID}` | SET | 24h | 用户关注的大 V ID 集合 |
| `user:follower_count:{userID}` | STRING | 无 | 用户粉丝数缓存 |
| `user:active:{userID}` | STRING | 72h | 登录时写入，推拉结合过滤僵尸粉用 |

**写入路径 -- FanoutWorker（`backend_repeat/internal/worker/fanoutworker.go`）：**

```go
// 1. 所有作者都写 user_videos:{authorID} ZSET (cap 50, 24h TTL)
redisClient.ZAdd(ctx, userVideosKey, redis.Z{Member: videoID, Score: createTime})
redisClient.ZRemRangeByRank(ctx, userVideosKey, 0, -(userVideosCap+1))

// 2. 查询作者粉丝数（Redis 缓存 → MySQL 兜底）
followerCount := getFollowerCount(ctx, authorID)

// 3. 大 V（>= 10000）→ 不 fanout，只保留发件箱
if followerCount >= BigVThreshold { return nil }

// 4. 过滤活跃粉丝（3 天内登录过，user:active:{id} TTL 72h）
followerIDs = filterActiveFollowers(ctx, followerIDs)

// 5. 普通用户 → 分批 fanout (每批 100 人，Pipeline 批量 ZADD)
for _, batch := range chunk(followerIDs, fanoutBatchSize) {
    pipe := redisClient.GetRedisClient().Pipeline()
    for _, fid := range batch {
        pipe.ZAdd(ctx, fmt.Sprintf("inbox:%d", fid), redis.Z{Member: videoID, Score: createTime})
        if rand.Float64() < trimChance {  // 1% 概率裁剪
            pipe.ZRemRangeByRank(ctx, fmt.Sprintf("inbox:%d", fid), 0, -(inboxCap+1))
        }
    }
    pipe.Exec(ctx)
}
```

**读取路径 -- ListByFollowing（`backend_repeat/internal/feed/service.go`）：**

```go
// Step 1: 并行读取 (1 RTT)
//   goroutine A: ZREVRANGEWITHSCORES inbox:{viewerID} (limit*2 条)
//   goroutine B: SMEMBERS following:bigv:{viewerID}

// Step 2: 公平提前终止判断 (1 RTT Pipeline)
//   Pipeline 批量查所有大V最新 1 条时间戳，取 max(newestAt)
//   只有当 max(newestAt) <= inboxOldest 时才跳过拉取
//   （旧逻辑只采样 1 个大V，以偏概全）

// Step 3: 活跃度优先拉取 (1 RTT Pipeline)
//   按 newestAt 降序排序大V，从最活跃的开始分配配额
//   needed = limit - len(pushStream)
//   跳过 newestAt <= inboxOldest 的冷大V（拉出来也排不到前面）
//   每个大V最多取 min(remaining, 10) 条，配额用完即止
//   所有大V的 ZREVRANGEWITHSCORES 合并为 1 次 Pipeline

// Step 4: 真正的 k-way merge 归并去重
//   构造 []streamCursor（每个流一个迭代器）
//   堆大小 = 流数量 K（非总元素数 N），O(N log K) 时间，O(K+limit) 空间
//   每次 pop 后从同一流 advance 取下一个入堆

// Step 5: GetVideoByIDs 三级缓存查完整视频 → buildFeedVideos 拼装响应
```

**各场景开销：**

| 场景 | Redis 次数 | 网络 RTT | 说明 |
|------|-----------|---------|------|
| 未登录或 Redis 不可用 | 0 | 0 | 降级 MySQL 子查询 |
| 没关注大 V | 2 | 1 | SMEMBERS 返回空，跳过拉取 |
| inbox 够用且所有大 V 无新内容 | 3 | 2 | Pipeline 批量探测所有大V确认无需拉取 |
| inbox 不够，需要拉取 | 3 | 2 | 探测+拉取合并为 1 次 Pipeline |
| 归并结果为空 | 3 | 2 | 降级 MySQL 子查询兜底 |

> 读取路径的 Redis 网络往返次数不随大 V 数量增长（Pipeline 将 O(N) 合并为 O(1)），整体约 3 次 RTT（并行读取 + Pipeline 探测 + Pipeline 拉取），与大 V 数量无关。

**关注回填（`backend_repeat/internal/worker/socialworker.go`）：**

关注普通用户时，从 `user_videos:{vloggerID}` 取最近 50 条，Pipeline 批量写入 `inbox:{followerID}`。取关时从 `following:bigv:{followerID}` SET 移除（不清理 inbox，靠自然 cap 淘汰）。

### 自检题

1. 三级缓存中 L1 的作用是什么？为什么不能只用 L2+L3？如果有 10 个 API 实例，L1 之间如何保持一致？
2. 限流脚本中为什么只在 count==1 时设置 PEXPIRE？如果每次都设置会怎样？
3. ZSET 时间线的 watermark 是什么？如何判断热/冷查询？watermark 会随时间变化吗？
4. singleflight 的 shared 返回值有什么含义？如果 shared=true 且原始请求返回了错误，所有等待者都会收到同一个错误吗？
5. 缓存穿透、击穿、雪崩分别是什么？本项目如何应对？还有哪些方案本项目没用但业界常用？
6. 为什么冷数据不回写 ZSET？如果某个冷视频突然爆火（被分享到微博），系统会怎样应对？
7. 推拉结合中 inbox 和 user_videos ZSET 的 cap 分别是多少？为什么 inbox 用概率裁剪而不是每次都裁剪？
8. 读取路径的恒定网络 RTT 是怎么实现的？如果关注了 100 个大V，Pipeline 的 payload 会不会过大？
9. 提前终止为什么要批量探测所有大V而不是只采样一个？给出一个只采样一个会误判的具体场景。
10. singleflight 和分布式锁有什么区别？本项目怎么用的？如果要部署 3 个 API 实例，哪些地方需要改？
11. go-cache 的过期清理是主动还是惰性的？如果 L1 中存了 10 万个 key，内存占用怎么控制？
12. MGet 批量查询相比逐个 Get 的优势是什么？如果 100 个 key 中有 50 个 miss，MGet 的返回值是什么样的？
13. 异步回写 Redis 用 `go func()` 启动 goroutine，如果这个 goroutine panic 了会怎样？怎么防护？

---

## 第3章：消息队列设计

### 3.1 Topic Exchange 设计

本项目使用 5 个 Topic Exchange，其中 `video.timeline.events` 绑定两个 Queue（分别处理时间线和 fanout），共 6 个 Queue：

| Exchange | Queue | Binding Key | 用途 |
|----------|-------|-------------|------|
| `social.events` | `social.events` | `social.*` | 关注/取关事件 |
| `like.events` | `like.events` | `like.*` | 点赞/取消点赞事件 |
| `comment.events` | `comment.events` | `comment.*` | 评论发布/删除事件 |
| `video.popularity.events` | `video.popularity.events` | `video.popularity.*` | 热度变更事件 |
| `video.timeline.events` | `video.timeline.update.queue` | `video.timeline.publish` | 视频发布事件（时间线） |
| `video.timeline.events` | `video.timeline.fanout.queue` | `video.timeline.fanout` | 视频发布事件（推送到粉丝收件箱） |

**Routing Key 设计：**

```go
// backend_repeat/internal/middleware/rabbitmq/likeMQ.go
likeLikeRK   = "like.like"    // 点赞
likeUnlikeRK = "like.unlike"  // 取消点赞
```

```go
// backend_repeat/internal/middleware/rabbitmq/socialMQ.go (推断)
// social.follow / social.unfollow
```

**Topic Exchange vs Direct Exchange：**

- Topic Exchange 支持通配符匹配（`*` 匹配一个词，`#` 匹配零或多个词）
- 本项目用 `social.*` 作为 binding key，可以匹配 `social.follow` 和 `social.unfollow`
- Direct Exchange 只能精确匹配 routing key

### 3.2 消息可靠性三层保证

**第一层：Producer -- 持久化消息**

```go
// backend_repeat/internal/middleware/rabbitmq/rabbitmq.go:PublishJSON
return r.Ch.PublishWithContext(ctx, exchange, routineKey, false, false, amqp.Publishing{
    ContentType:  "application/json",
    DeliveryMode: amqp.Persistent,  // 持久化消息，写入磁盘
    Body:         body,
    Timestamp:    time.Now(),
})
```

`DeliveryMode: amqp.Persistent` 保证消息被写入磁盘，即使 RabbitMQ 重启也不会丢失。

**第二层：Broker -- 持久化 Exchange 和 Queue**

```go
// backend_repeat/internal/middleware/rabbitmq/rabbitmq.go:DeclareTopic
// 声明 Exchange，durable=true
r.Ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil)
// 声明 Queue，durable=true
q, err := r.Ch.QueueDeclare(queue, true, false, false, false, nil)
```

`durable: true` 保证 Exchange 和 Queue 的元数据持久化到磁盘。

**第三层：Consumer -- 手动 Ack/Nack**

```go
// backend_repeat/internal/worker/likeworker.go:Run
deliveries, err := w.rbq.Ch.Consume(
    w.queue,
    "",     // consumer: 空字符串由 RabbitMQ 自动生成
    false,  // autoAck: false，手动确认
    false,  // exclusive
    false,  // noLocal
    false,  // noWait
    nil,
)

// handleDelivery
if err := w.process(ctx, d.Body); err != nil {
    _ = d.Nack(false, true)  // 处理失败，重新入队
    return
}
_ = d.Ack(false)  // 处理成功，确认消费
```

`autoAck=false` + 手动 Ack 保证消息在处理成功前不会被队列移除。

### 3.3 Outbox 模式

Outbox 模式解决了"数据库写入成功但 MQ 投递失败"导致的事件丢失问题。

**写入阶段 -- `PublishVideo`：**

```go
// backend_repeat/internal/video/video_repo.go:PublishVideo
err := r.db.WithContext(ctx).Transaction(func(db *gorm.DB) error {
    // 1. 写入视频记录
    if err := db.Create(video).Error; err != nil {
        return err
    }
    // 2. 写入 outbox 消息（同一事务）
    msg := OutboxMsg{
        VideoID:    video.ID,
        EventType:  "video_published",
        Status:     "pending",
        CreateTime: video.CreateTime,
    }
    if err := db.Create(&msg).Error; err != nil {
        return err
    }
    return nil
})
```

视频记录和 outbox 消息在同一个 MySQL 事务中写入，保证原子性。

**轮询投递阶段 -- `StartOutboxPoller`（双投递）：**

```go
// backend_repeat/internal/worker/outboxworker.go:StartOutboxPoller
// 签名：func StartOutboxPoller(db *gorm.DB, tmq *rabbitmq.TimelineMQ, fmq *rabbitmq.FanoutMQ)
go func() {
    for {
        var msgs []video.OutboxMsg
        db.Where("status = ?", "pending").Order("create_time ASC").Limit(100).Find(&msgs)
        if len(msgs) == 0 { time.Sleep(1 * time.Second); continue }
        for _, msg := range msgs {
            // 先投递 TimelineMQ（写全局时间线）
            err1 := tmq.PublishVideo(ctx, msg.VideoID, msg.CreateTime)
            // 再投递 FanoutMQ（推送到粉丝收件箱）
            err2 := fmq.PublishFanout(ctx, msg.VideoID, msg.AuthorID, msg.CreateTime)
            // 两者都成功才删除 outbox 记录
            if err1 == nil && err2 == nil {
                db.Delete(&msg)
            }
        }
    }
}()
```

**消费写入阶段 -- `StartConsumer`：**

```go
// backend_repeat/internal/worker/outboxworker.go:StartConsumer
for msg := range msgs {
    var val rabbitmq.TimelineEvent
    json.Unmarshal(msg.Body, &val)
    // ZADD 写入时间线 ZSET
    redisClient.ZAdd(ctx, timelineKey, redis.Z{
        Member: fmt.Sprintf("%d", val.VideoID),
        Score:  float64(val.CreateTime),
    })
    // 裁剪，只保留最新 1000 条
    redisClient.ZRemRangeByRank(ctx, timelineKey, 0, -1001)
    msg.Ack(false)
}
```

**为什么不用直接发 MQ？**

如果先写 DB 事务、再发 MQ，存在以下风险：

```
1. DB 事务提交成功
2. 发 MQ 失败（网络抖动、MQ 宕机）
3. 视频已入库，但时间线 ZSET 永远不会有这条视频
```

Outbox 模式通过"先写 outbox 表、后轮询投递"保证即使 MQ 暂时不可用，事件也不会丢失。

**双投递的失败语义**：

OutboxPoller 对每条消息先投递 TimelineMQ、再投递 FanoutMQ。两者都成功才硬删除 outbox 记录：

| 场景 | TimelineMQ | FanoutMQ | 行为 |
|------|-----------|----------|------|
| 全部成功 | ✓ | ✓ | 硬删除 outbox 记录 |
| TimelineMQ 失败 | ✗ | — | 跳过（不删除），下个轮询周期重试 |
| TimelineMQ 成功 + FanoutMQ 失败 | ✓ | ✗ | 跳过（不删除），下个轮询周期**重新投递两条** |

注意：第三种场景下 TimelineMQ 会被重复投递，Timeline 消费者（`StartConsumer`）的幂等性（ZADD 同一 member 只保留最新 score）保证重复投递无副作用。

### 3.4 幂等消费

MQ 的 at-least-once 语义可能导致消息重复投递，消费者必须保证幂等性。

**LikeWorker -- 事务内幂等消费：**

```go
// backend_repeat/internal/worker/likeworker.go:applyLike
return w.likeRepo.Transaction(ctx, func(tx *gorm.DB) error {
    created, err := w.likeRepo.LikeIgnoreDuplicateInTx(ctx, tx, &video.Like{
        VideoID:   videoID,
        AccountID: userID,
        CreatedAt: time.Now(),
    })
    if err != nil {
        return err
    }
    if !created {
        return nil  // 重复消息，事务空提交
    }
    if err := w.videoRepo.ChangeLikesCount(ctx, tx, videoID, 1); err != nil {
        return err
    }
    return w.videoRepo.ChangePopularity(ctx, tx, videoID, 1)
})
```

事务保证 INSERT + UPDATE likes_count + UPDATE popularity 原子执行。任一步失败则整个事务回滚，MQ Nack 重试。避免了"②成功③失败时重试跳过③"的问题。

```go
// backend_repeat/internal/video/like_repo.go:LikeIgnoreDuplicateInTx
func (r *LikeRepository) LikeIgnoreDuplicateInTx(ctx context.Context, tx *gorm.DB, like *Like) (created bool, err error) {
    err = tx.WithContext(ctx).Create(like).Error
    if err == nil {
        return true, nil
    }
    if isDuplicateKey(err) {
        return false, nil  // 1062 重复键，幂等返回
    }
    return false, err
}
```

**SocialWorker -- 1062 幂等忽略：**

```go
// backend_repeat/internal/worker/socialworker.go:process
case "follow":
    err := w.repo.Follow(ctx, &social.Social{
        FollowerID: evt.FollowerID,
        VloggerID:  evt.VloggerID,
    })
    if err == nil {
        return nil
    }
    // 处理 MySQL 1062 唯一键冲突
    var mysqlErr *mysql.MySQLError
    if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
        return nil  // 幂等忽略
    }
    return err
```

**CommentWorker -- `CreateComment`：**

```go
// backend_repeat/internal/worker/commentworker.go:applyPublish
c := &video.Comment{
    Username: strings.TrimSpace(evt.Username),
    VideoID:  evt.VideoID,
    AuthorID: evt.AuthorID,
    Content:  strings.TrimSpace(evt.Content),
}
if err := w.commentRepo.CreateComment(ctx, c); err != nil {
    return err
}
return w.videoRepo.ChangePopularity(ctx, nil, evt.VideoID, 1)
```

### 3.5 MQ 优先 + 同步降级

本项目的核心写操作（点赞、评论）都采用"MQ 优先 + 同步降级"策略。

```go
// backend_repeat/internal/video/like_service.go:Like
likeMQSent := false
popularityMQSent := false

// 尝试 MQ 投递
if s.likeMQ != nil {
    if err := s.likeMQ.Like(ctx, accountID, videoID); err == nil {
        likeMQSent = true
    }
}
if s.popularityMQ != nil {
    if err := s.popularityMQ.Update(ctx, videoID, 1); err == nil {
        popularityMQSent = true
    }
}

// 两条 MQ 都成功 → 异步路径
if likeMQSent && popularityMQSent {
    return nil
}

// LikeMQ 失败 → 同步 MySQL 事务
if !likeMQSent {
    err := s.likeRepo.Transaction(ctx, func(tx *gorm.DB) error {
        s.likeRepo.LikeInTx(ctx, tx, like)
        s.videoRepo.ChangeLikesCount(ctx, tx, videoID, 1)
        s.videoRepo.ChangePopularity(ctx, tx, videoID, 1)
        return nil
    })
}

// PopularityMQ 失败 → 直接调用 UpdatePopularityCache
if !popularityMQSent {
    UpdatePopularityCache(ctx, s.cache, videoID, 1)
}
```

**MQ 优先路径 vs 降级路径的数据一致性区别：**

- **MQ 优先路径**：API 只投递消息，不写 MySQL。MySQL 写入由 Worker 异步完成。一致性依赖 MQ 可靠性和 Worker 正确性。
- **降级路径**：API 同步写 MySQL 事务，保证强一致性。但此时没有异步 Worker 参与，写入延迟更高。

### 3.6 生产者-消费者分离

```
API 进程（生产者）：
    likeMQ.Like()         --> like.events exchange
    popularityMQ.Update() --> video.popularity.events exchange
    commentMQ.Publish()   --> comment.events exchange
    socialMQ.Follow()     --> social.events exchange
    tmq.PublishVideo()    --> video.timeline.events exchange (routing: video.timeline.publish)
    fmq.PublishFanout()   --> video.timeline.events exchange (routing: video.timeline.fanout)

Worker 进程（消费者）：
    LikeWorker.Run()         <-- like.events queue
    PopularityWorker.Run()   <-- video.popularity.events queue
    CommentWorker.Run()      <-- comment.events queue
    SocialWorker.Run()       <-- social.events queue
    FanoutWorker.Run()       <-- video.timeline.fanout.queue (推拉结合 fanout)
    StartConsumer()          <-- video.timeline.update.queue (写全局时间线)
```

注意：`StartOutboxPoller`、`StartConsumer`（时间线消费者）和 `FanoutWorker` 中，`StartOutboxPoller` 和 `StartConsumer` 运行在 API 进程中（`backend_repeat/internal/http/router.go:SetRouter`），`FanoutWorker` 运行在 Worker 进程中（`backend_repeat/cmd/worker/main.go`）。FanoutMQ 和 TimelineMQ 复用同一个 Topic Exchange（`video.timeline.events`），但各自使用独立的 routing key 和队列，实现发布-订阅分离。

### 自检题

1. Topic Exchange 和 Direct Exchange 的区别？什么场景下 Topic 比 Direct 更合适？
2. 为什么消息要设为 Persistent？Persistent 消息一定不会丢吗？（提示：fsync 时机）
3. Outbox 模式解决了什么问题？和 2PC（两阶段提交）相比有什么优劣？
4. 消费者 `Nack(false, true)` 的两个参数分别是什么意思？如果第二个参数传 false 会怎样？
5. 1062 错误码代表什么？为什么是幂等的基石？如果表没有唯一索引，怎么实现幂等？
6. MQ 优先路径和降级路径的数据一致性有什么区别？降级路径会不会导致 Worker 重复处理？
7. At-least-once 和 Exactly-once 的区别？为什么分布式系统中 Exactly-once 很难实现？
8. 如果 OutboxPoller 投递成功但 DELETE 失败会怎样？消费者如何应对重复消息？
9. Outbox 双投递的顺序是什么？为什么先 TimelineMQ 再 FanoutMQ？如果 FanoutMQ 持续失败，outbox 表会膨胀吗？
10. RabbitMQ 的 prefetch 机制是什么？prefetchCount=50 意味着什么？和 Kafka 的 consumer group 有什么本质区别？
11. 如果消费者处理一条消息耗时 10 秒，RabbitMQ 会认为消费者挂了吗？heartbeat 和 ack timeout 的关系是什么？
12. 死信队列（DLX）是什么？本项目为什么没有用？什么场景下应该引入？

---

## 第4章：Feed 流设计

### 4.1 推拉模型分析

本项目实现了**推拉结合**（Push-Pull Hybrid）模型：

| 接口 | 数据来源 | 模型 | 说明 |
|------|---------|------|------|
| `ListLatest` | Redis ZSET + MySQL | 拉模型 | 冷热分离，热数据从 ZSET 取，冷数据从 MySQL 取 |
| `ListByFollowing` | inbox ZSET + 大V发件箱 ZSET + MySQL | **推拉结合** | 推路径读 inbox，拉路径读大V发件箱，k-way merge 归并 |
| `ListByPopularity` | Redis ZSET + MySQL | 拉模型 | 60 个窗口 ZUNIONSTORE 合并，降级 MySQL |

**三种模型对比：**

- **推模型**（fanout-on-write）：用户发布视频时，主动推送到每个粉丝的收件箱。优点是读取快（直接读收件箱），缺点是写扩散（大 V 发视频要推给百万粉丝）。
- **拉模型**（fanout-on-read）：用户请求时，实时从数据源拉取。优点是写入快（只写一份），缺点是读取慢（需要实时计算）。
- **推拉结合**：普通用户（粉丝 < 阈值）用推模型，发布时写入粉丝收件箱；大 V（粉丝 >= 阈值）用拉模型，不推送，读取时从发件箱拉取。读取时两路数据归并排序，对用户无感知。

本项目选择推拉结合的原因：大 V 的写扩散成本过高（1 亿粉丝发一条 = 1 亿次写入），推拉结合平衡了写放大和读放大。详细实现见 4.6 节。

### 4.2 冷热分离

核心实现在 `backend_repeat/internal/feed/service.go:ListLatest`。

**Watermark 的确定：**

```go
// ZSET 中最老一条的 score 就是 watermark
zsetTail, err := f.rediscache.ZRangeWithScores(ctx, "feed:global_timeline", 0, 0)
// index=0 拿分数最小的（最老的数据）
watermark := int64(zsetTail[0].Score)
```

`ZRangeWithScores(key, 0, 0)` 返回 ZSET 中排名最低（score 最小）的成员，其 score 就是 watermark。

**热/冷判断逻辑：**

```go
reqTime := time.Now().UnixMilli()
if !latestBefore.IsZero() {
    reqTime = latestBefore.UnixMilli()  // 上页最后一条的时间
}

if reqTime <= watermark {
    // 冷查询：请求的时间 <= ZSET 最老一条的时间
    // 说明数据已经超出 ZSET 覆盖范围，需要查 MySQL
    sfKey := fmt.Sprintf("sf:cold:listLatest:%d:%d", limit, reqTime)
    v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
        return f.repo.ListLatest(ctx, limit, latestBefore)
    })
    // 不回写 ZSET，防止冷数据污染热点时间线
} else {
    // 热查询：请求的时间 > watermark
    // 数据在 ZSET 覆盖范围内，从 Redis 取
    maxScore := fmt.Sprintf("%d", reqTime-1)  // 防重复
    videoIDStr, err := f.rediscache.ZRevRangeByScore(ctx, "feed:global_timeline", maxScore, "-inf", 0, int64(limit))
}
```

**跨边界拼接：**

```go
// 热数据不足 limit 条时，补冷尾
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
    coldVideos := v.([]*video.Video)
    baseVideos = append(baseVideos, coldVideos...)
}
```

**冷数据不回写 ZSET 的原因：**

冷数据通常是历史数据，访问频率低。如果回写 ZSET，会挤掉 ZSET 中最新的 1000 条热点数据（ZSET 有容量上限），导致热点时间线被污染。

### 4.3 游标分页三种实现

#### 4.3.1 时间游标 -- `ListLatest`

```go
// backend_repeat/internal/feed/repo.go:ListLatest
query := repo.db.WithContext(ctx).Model(&video.Video{}).Order("create_time DESC")
if !latestBefore.IsZero() {
    query = query.Where("create_time < ?", latestBefore)
}
query.Limit(limit).Find(&videos)
```

- 游标：上页最后一条的 `create_time`
- 优点：简单高效
- 缺点：并列时不稳定（同一秒创建的视频可能被跳过或重复）

#### 4.3.2 双字段游标 -- `ListLikesCount`

```go
// backend_repeat/internal/feed/repo.go:ListLikesCountWithCursor
query := repo.db.WithContext(ctx).Model(&video.Video{}).Order("likes_count DESC, id DESC")
if cursor != nil {
    query = query.Where(
        "(likes_count < ?) OR (likes_count = ? AND id < ?)",
        cursor.LikesCount, cursor.LikesCount, cursor.ID,
    )
}
```

- 游标：`(likes_count, id)` 双字段
- 解决并列问题：多个视频点赞数相同时，用 `id` 作为 tiebreaker，保证分页稳定性
- ORDER BY 子句和 WHERE 子句的字段必须对应

#### 4.3.3 三字段游标 -- `ListByPopularity`

```go
// backend_repeat/internal/feed/repo.go:ListByPopularity
query := repo.db.WithContext(ctx).Model(&video.Video{}).Order("popularity DESC, create_time DESC, id DESC")
if !timeBefore.IsZero() && idBefore > 0 {
    query = query.Where(
        "(popularity < ?) OR (popularity = ? AND create_time < ?) OR (popularity = ? AND create_time = ? AND id < ?)",
        popularityBefore,
        popularityBefore, timeBefore,
        popularityBefore, timeBefore, idBefore,
    )
}
```

- 游标：`(popularity, create_time, id)` 三字段
- 三重并列处理：热度相同时按创建时间排序，创建时间也相同时按 ID 排序

#### 游标分页 vs Offset 分页

| 维度 | 游标分页 | Offset 分页 |
|------|---------|------------|
| 时间复杂度 | O(log N + limit) | O(N + limit)（大 offset 时退化） |
| 稳定性 | 稳定（不受插入/删除影响） | 不稳定（插入新数据后偏移量错位） |
| 适用场景 | 无限滚动、实时 Feed | 后台管理页、跳页 |

### 4.4 热榜排行

核心实现在 `backend_repeat/internal/feed/service.go:ListByPopularity`。

**Redis 热榜路径：**

```go
// 合并过去 60 个 1 分钟窗口，越老的窗口权重越小
const win = 60
const decay = 0.95 // 每分钟衰减 5%
keys := make([]string, 0, win)
weights := make([]float64, 0, win)
for i := 0; i < win; i++ {
    keys = append(keys, "hot:video:1m:"+asOf.Add(-time.Duration(i)*time.Minute).Format("200601021504"))
    weights = append(weights, math.Pow(decay, float64(i)))
}
dest := "hot:video:merge:1m:" + asOf.Format("200601021504")

// 缓存合并结果 2 分钟
exists, _ := f.rediscache.Exists(opCtx, dest)
if !exists {
    _ = f.rediscache.ZUnionStoreWithWeights(opCtx, dest, keys, weights, "SUM")
    _ = f.rediscache.Expire(opCtx, dest, 2*time.Minute)
}

// 分页取数据
start := int64(offset)
stop := start + int64(limit) - 1
members, err := f.rediscache.ZRevRange(opCtx, dest, start, stop)
```

**as_of + offset 稳定分页：**

- `as_of`：客户端传回服务器返回的分钟时间戳，保证翻页时基准时间不变
- `offset`：从合并后的 ZSET 中按排名偏移取数据
- 第一页 `as_of=0, offset=0`，后续页使用响应中的 `AsOf` 和 `NextOffset`

**MySQL 降级路径：**

```go
// 三字段游标分页
videos, err := f.repo.ListByPopularity(ctx, limit, latestPopularity, latestBefore, latestIDBefore)
```

Redis 不可用时降级到 MySQL 三字段游标分页，保证功能可用。

### 4.5 个性化 vs 公共接口

**SoftJWTAuth -- 可选认证：**

```go
// backend_repeat/internal/middleware/jwt/jwt.go:SoftJWTAuth
func SoftJWTAuth(accountRepo *account.AccountRepository, cache *rediscache.Client) gin.HandlerFunc {
    return func(c *gin.Context) {
        authHeader := c.GetHeader("Authorization")
        if authHeader == "" {
            c.Next()  // 没有 token 也放行
            return
        }
        // 有 token 则解析身份
        parts := strings.SplitN(authHeader, " ", 2)
        tokenString := parts[1]
        claims, err := auth.ParseToken(tokenString)
        // ... 验证并写入 context
        check(c, claims, tokenString, accountRepo, cache)
    }
}
```

**登录用户 vs 未登录用户：**

```go
// backend_repeat/internal/feed/service.go:buildFeedVideos
func (f *FeedService) buildFeedVideos(ctx context.Context, videos []*video.Video, viewerAccountID uint) ([]FeedVideoItem, error) {
    videoIDs := make([]uint, len(videos))
    for i, v := range videos {
        videoIDs[i] = v.ID
    }
    // 批量查询当前用户的点赞状态
    likedMap, err := f.likeRepo.BatchGetLiked(ctx, videoIDs, viewerAccountID)
    // ...
    for _, video := range videos {
        feedVideos = append(feedVideos, FeedVideoItem{
            // ...
            IsLiked: likedMap[video.ID],  // 登录用户有真实值，未登录用户 viewerAccountID=0 返回空 map
        })
    }
}
```

路由配置：

```go
// backend_repeat/internal/http/router.go:SetRouter
feedGroup := r.Group("/feed")
feedGroup.Use(jwt.SoftJWTAuth(accountRepository, cache))  // 公共接口用 SoftJWT
{
    feedGroup.POST("/listLatest", feedHandler.ListLatest)
    feedGroup.POST("/listLikesCount", feedHandler.ListLikesCount)
    feedGroup.POST("/listByPopularity", feedHandler.ListByPopularity)
}
protectedFeedGroup := feedGroup.Group("")
protectedFeedGroup.Use(jwt.JWTAuth(accountRepository, cache))  // 私有接口用硬 JWT
{
    protectedFeedGroup.POST("/listByFollowing", feedHandler.ListByFollowing)
}
```

### 4.6 推拉结合实现

核心实现在 `backend_repeat/internal/feed/service.go:ListByFollowing`（读取路径）和 `backend_repeat/internal/worker/fanoutworker.go`（写入路径）。

**大 V 阈值设计：**

默认 10,000 粉丝。参考 Twitter（~500,000）、微博（~5,000-10,000）、Stream-Framework（无硬编码，由业务层决定）。可通过 Redis 配置中心动态调整。

**写入路径 -- FanoutWorker：**

```go
// backend_repeat/internal/worker/fanoutworker.go:process
// 1. 所有作者都写 user_videos:{authorID} ZSET (cap 50, 24h TTL)
// 2. 查询作者粉丝数（Redis 缓存 → MySQL CountFollowers 兜底）
// 3. >= 10000 → 大V，不 fanout（只写发件箱）
// 4. < 10000 → 普通用户，过滤活跃粉丝（3 天内登录过），分批 fanout
//    - Pipeline 批量 EXISTS user:active:{id}，只推活跃粉丝
//    - 每批 100 人，Redis Pipeline 批量 ZADD
//    - 1% 概率触发 ZREMRANGEBYRANK 裁剪 inbox (cap 500)
```

**读取路径 -- ListByFollowing 五步流程：**

```go
// backend_repeat/internal/feed/service.go:ListByFollowing

// Step 1: 并行读取 (1 RTT)
//   goroutine A: ZREVRANGEWITHSCORES inbox:{viewerID} (limit*2 条)
//   goroutine B: SMEMBERS following:bigv:{viewerID}

// Step 2: 公平提前终止判断 (1 RTT Pipeline)
//   Pipeline 批量查所有大V最新 1 条时间戳，取 max(newestAt)
//   只有 max(newestAt) <= inboxOldest 时才跳过拉取

// Step 3: 活跃度优先拉取 (与 Step 2 合并为 1 RTT)
//   按 newestAt 降序排序大V，从最活跃开始分配配额
//   needed = limit - len(pushStream)
//   跳过 newestAt <= inboxOldest 的冷大V
//   每个大V最多 min(remaining, 10) 条，配额用完即止

// Step 4: 真正的 k-way merge 归并去重
//   streamCursor 迭代器 + mergeHeap（堆大小 = 流数量 K）
//   O(N log K) 时间复杂度，O(K) 空间复杂度

// Step 5: GetVideoByIDs 三级缓存 → buildFeedVideos 拼装响应
```

**读取路径优化要点：**

| 优化 | 效果 |
|------|------|
| 粉丝活跃度过滤 | 只推给 3 天内登录过的粉丝（Pipeline EXISTS user:active:{id}），僵尸粉不推，减少写放大 |
| 并行读取 | inbox 和 bigV 列表 goroutine 并行，串行 2 RTT → 并行 1 RTT |
| 公平提前终止 | Pipeline 批量探测所有大V最新时间戳取 max，避免只采样 1 个大V的以偏概全问题 |
| 拉取配额跳过冷大V | newestAt <= inboxOldest 的大V不分配配额，避免无意义 Redis 查询 |
| 活跃度优先拉取 | 按 newestAt 降序分配配额，最活跃大V优先展示，而非 uniform 一刀切 |
| Pipeline 批量 | N 个大V的探测+拉取合并为 1 次 Redis 往返，RTT 不随大V数量增长 |
| 真正的 k-way merge | 堆大小 = K（流数量），每次 pop 后从同一流 advance 取下一个入堆，O(N log K) 时间，O(K+limit) 空间 |
| 恒定网络 RTT | 无论大V数量多少，读取路径 Redis 网络往返恒定约 3 次（不随大V数量增长） |

**k-way merge 实现细节**（`internal/feed/service.go`）：

```go
// 数据结构
type mergeItem struct {
    VideoID    uint
    CreateTime time.Time
    source     string  // 来源标识（debug 用）
}

type streamCursor struct {
    items  []mergeItem
    pos    int
    source string
}

func (s *streamCursor) peek() mergeItem { return s.items[s.pos] }
func (s *streamCursor) advance()        { s.pos++ }
func (s *streamCursor) exhausted() bool { return s.pos >= len(s.items) }

// container/heap 接口实现 -- 按 CreateTime 降序（最大堆）
type mergeHeap []*streamCursor

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
    return h[i].peek().CreateTime.After(h[j].peek().CreateTime)
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(*streamCursor)) }
func (h *mergeHeap) Pop() any     { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }
```

**归并流程**：

```go
// 1. 初始化：将所有非空 stream 的 cursor 压入堆
h := &mergeHeap{}
heap.Init(h)
for _, cursor := range streams {
    if !cursor.exhausted() {
        heap.Push(h, cursor)
    }
}

// 2. 归并：每次弹出堆顶（最大 CreateTime），去重后追加到结果
seen := make(map[uint]bool)
var result []mergeItem
for h.Len() > 0 && len(result) < limit {
    cursor := heap.Pop(h).(*streamCursor)
    item := cursor.peek()
    if !seen[item.VideoID] {
        seen[item.VideoID] = true
        result = append(result, item)
    }
    cursor.advance()
    if !cursor.exhausted() {
        heap.Push(h, cursor)
    }
}
```

**复杂度分析**：
- 时间：O(N log K)，N = 总视频数，K = 流数量（inbox + 大V 数量）。每次 pop/push 操作 O(log K)
- 空间：O(K) 堆 + O(N) seen map + O(limit) 结果集
- K=1 退化：只有 inbox 没有大V 时，堆大小为 1，退化为简单遍历，O(N)

**关注/取关维护（`backend_repeat/internal/worker/socialworker.go`）：**

```go
// 关注大V → SAdd vloggerID 到 following:bigv:{followerID} SET
// 关注普通用户 → backfillInbox：从 user_videos:{vloggerID} 取最近 50 条写入 inbox:{followerID}
// 取关 → SRem vloggerID 从 following:bigv:{followerID} SET（不清理 inbox，靠自然 cap 淘汰）
```

**降级策略：** Redis 不可用时 `ListByFollowing` 自动降级到 MySQL 子查询模式（`listByFollowingFromDB`）。FanoutWorker 的 Redis 写入失败只记日志，不影响视频发布主流程。

### 自检题

1. 推拉结合怎么实现的？大 V 阈值怎么定？阈值设太高或太低分别有什么问题？
2. watermark 如何确定？为什么用 ZSET 最老一条的 score？如果 ZSET 为空，watermark 是什么？
3. 双字段游标如何解决并列问题？为什么第二个字段必须是唯一的？
4. ZUNIONSTORE 的时间复杂度？如果 60 个窗口每个有 1000 个 member，合并开销是多少？
5. 为什么热度排行用 as_of + offset 而不是游标？如果用游标会有什么问题？
6. 冷热分离时，跨边界拼接的逻辑是什么？如果热数据恰好 0 条怎么办？
7. SoftJWTAuth 和 JWTAuth 的区别？SoftJWTAuth 下 token 无效（格式错误）时应该放行还是拒绝？
8. `buildFeedVideos` 中 `is_liked` 是怎么批量查询的？为什么不逐个查询？
9. k-way merge 的时间复杂度？堆大小为什么是 K 而不是 N？如果 K=1（只有 inbox 没有大V），退化成什么？
10. 关注大V和关注普通用户时，inbox 分别怎么处理？取关时为什么不清理 inbox？
11. 提前终止为什么要批量探测所有大V？活跃度优先拉取的配额分配策略是什么？
12. 推路径为什么只推活跃粉丝？怎么判定活跃？僵尸粉怎么兜底？
13. 拉取配额分配时为什么跳过冷大V？判断条件是什么？
14. 游标分页在"跳到第 N 页"场景下有什么局限？怎么解决？
15. 如果用户关注了 500 个大V，读取路径的 Pipeline payload 会有多大？有没有上限保护？

---

## 第5章：认证、安全与限流

### 5.1 JWT + Redis Token 吊销

**签发 -- `GenerateToken`：**

```go
// backend_repeat/internal/auth/jwt.go:GenerateToken
func GenerateToken(accountID uint, Username string) (string, error) {
    now := time.Now()
    claims := Claims{
        AccountID: accountID,
        Username:  Username,
        RegisteredClaims: jwt.RegisteredClaims{
            ExpiresAt: jwt.NewNumericDate(now.Add(24 * time.Hour)),  // 24h 过期
            IssuedAt:  jwt.NewNumericDate(now),
            NotBefore: jwt.NewNumericDate(now),
        },
    }
    token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)  // HS256 对称签名
    return token.SignedString(jwtSecret())
}
```

**存储 -- `Login`：**

```go
// backend_repeat/internal/account/service.go:Login
// MySQL 存储 Token
account.Token = tokenstring
s.accountRepository.Login(ctx, account)

// Redis 缓存 Token
s.cache.SetBytes(cancelctx, fmt.Sprintf("account:%d", account.ID),
    []byte(tokenstring), 24*time.Hour)
```

**验证 -- `check` 函数：**

```go
// backend_repeat/internal/middleware/jwt/jwt.go:check
func check(c *gin.Context, claims *auth.Claims, tokenString string, accountRepo *account.AccountRepository, cache *rediscache.Client) {
    key := fmt.Sprintf("account:%d", claims.AccountID)

    // 先查 Redis
    if cache != nil {
        trueToken, err := cache.GetBytes(cacheCtx, key)
        if err == nil {
            if string(trueToken) != tokenString {
                c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "token has been revoked"})
                return
            }
            // token 匹配，验证通过
            c.Set("accountID", claims.AccountID)
            c.Next()
            return
        }
    }

    // Redis 异常，降级 MySQL
    accountInfo, err := accountRepo.FindByID(c.Request.Context(), claims.AccountID)
    if err != nil || accountInfo.Token == "" || accountInfo.Token != tokenString {
        c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "token has been revoked"})
        return
    }

    // 尝试回写 Redis
    cache.SetBytes(cacheCtx, key, []byte(accountInfo.Token), time.Hour*24)
    c.Set("accountID", accountInfo.ID)
    c.Next()
}
```

**吊销 -- `Logout`：**

```go
// backend_repeat/internal/account/service.go:Logout
func (s *AccountService) Logout(ctx context.Context, id uint) error {
    // 清除 Redis 缓存
    s.cache.Del(cacheCtx, fmt.Sprintf("account:%d", account.ID))
    // 清除 MySQL Token
    return s.accountRepository.Logout(ctx, account.ID)
}
```

**吊销流程：** 用户登出时，同时清除 Redis 中的 token 缓存和 MySQL 中的 token 字段。后续请求携带旧 token 时，Redis 查询返回空（或新 token），比对失败，返回 401。

### 5.2 SoftJWTAuth

```go
// backend_repeat/internal/middleware/jwt/jwt.go:SoftJWTAuth
// 与 JWTAuth 的区别：没有 Authorization header 时不 Abort，而是 c.Next() 放行
```

**使用场景：**

- 公共接口（`/feed/listLatest` 等）：未登录用户也能访问，但登录用户能拿到 `is_liked` 个性化数据
- 私有接口（`/like/like` 等）：必须登录才能操作，使用硬 `JWTAuth`

### 5.3 TOCTOU 竞态处理

**TOCTOU（Time-of-Check to Time-of-Use）**：在"检查"和"使用"之间的时间窗口内，状态可能被其他线程/进程改变。

**点赞 -- 移除前置检查：**

```go
// backend_repeat/internal/video/like_service.go:Like
// 代码注释明确说明：
// 移除前置 IsLiked 检查，依赖唯一索引 (video_id, account_id) 兜底，消除 TOCTOU 竞态窗口

// 直接投递 MQ 或同步写入，不检查是否已点赞
like := &Like{VideoID: videoID, AccountID: accountID, CreatedAt: time.Now()}
// 依赖 LikeIgnoreDuplicateInTx 的 1062 检测保证幂等（事务内执行）
```

**关注 -- 保留预检查：**

```go
// backend_repeat/internal/social/service.go:Follow
// 代码注释明确说明：
// IsFollowed 预检查存在 TOCTOU 窗口，但 uniqueIndex 兜底防止重复插入。
// 保留预检查是为了给用户明确的错误提示（"已经关注了" vs 数据库 1062 错误）

isFollowed, err := s.repo.IsFollowed(ctx, social)
if isFollowed {
    return errors.New("already followed")  // 用户友好提示
}
// 即使两个并发请求都通过了预检查，uniqueIndex 会兜底
return s.repo.Follow(ctx, social)
```

**设计取舍：**

- 点赞：移除预检查，完全依赖唯一索引。原因是点赞是高频操作，预检查的额外 DB 查询成本不值得。
- 关注：保留预检查，给用户友好提示。原因是关注是低频操作，用户体验更重要。

### 5.4 限流中间件

```go
// backend_repeat/internal/middleware/ratelimit/ratelimit.go:Limit
func Limit(cache *rediscache.Client, keyPrefix string, maxRequests int64, window time.Duration, keyFunc KeyFunc) gin.HandlerFunc {
    return func(c *gin.Context) {
        subject, ok := keyFunc(c)  // 获取限流主体
        key := buildKey(keyPrefix, subject)
        count, err := cache.IncrementWithExpire(c.Request.Context(), key, window)
        if count > maxRequests {
            c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "too many requests"})
            return
        }
        c.Next()
    }
}
```

**Key 设计：**

```go
// 例：feedsystem:ratelimit:account_login:192.168.1.1
func buildKey(keyPrefix, subject string) string {
    return fmt.Sprintf("feedsystem:ratelimit:%s:%s", keyPrefix, strings.TrimSpace(subject))
}
```

**双维度限流：**

```go
// backend_repeat/internal/middleware/ratelimit/ratelimit.go
func KeyByIP(c *gin.Context) (string, bool) {
    ip := strings.TrimSpace(c.ClientIP())  // 按 IP 限流
    return ip, true
}

func KeyByAccount(c *gin.Context) (string, bool) {
    accountID, err := jwt.GetAccountID(c)  // 按用户 ID 限流
    return strconv.FormatUint(uint64(accountID), 10), true
}
```

**路由中的限流配置（双策略）：**

```go
// backend_repeat/internal/http/router.go:SetRouter
// 固定窗口：登录/注册（低频，简单）
loginLimiter    := ratelimit.Limit(cache, "account_login",    10, time.Minute, ratelimit.KeyByIP)
registerLimiter := ratelimit.Limit(cache, "account_register", 10, time.Minute, ratelimit.KeyByIP)
// 滑动窗口：点赞/评论/关注（高频，避免边界突刺）
likeLimiter     := ratelimit.SlidingWindowLimitByAccount(cache, "like_write",    30, time.Minute)
commentLimiter  := ratelimit.SlidingWindowLimitByAccount(cache, "comment_write", 10, time.Minute)
socialLimiter   := ratelimit.SlidingWindowLimitByAccount(cache, "social_write",  20, time.Minute)
```

**固定窗口临界问题 → 滑动窗口解决：**

固定窗口假设限流为 10 次/分钟：
- 在第 59 秒用完 10 次，在第 60 秒（窗口到期）又可以发 10 次 → 2 秒内 20 次（2x 突发）
- 滑动窗口统计"过去 window 时间内的实际请求数"，任意时刻严格不超 limit

### 5.5 JWT 安全隐患

1. **HS256 对称签名**：`backend_repeat/internal/auth/jwt.go` 使用 `jwt.SigningMethodHS256`。微服务架构建议使用 RS256（非对称），避免每个服务都持有密钥。

2. **硬编码默认密钥**：`const defaultJWTSecret = "feedsystem_secret"`。生产环境必须通过环境变量 `JWT_SECRET` 覆盖。

3. **24h 过期**：`ExpiresAt: jwt.NewNumericDate(now.Add(24 * time.Hour))`。建议实现 access token（短期）+ refresh token（长期）分离。

### 自检题

1. JWT 吊销的实现方式有哪些？本项目用的哪种？和黑名单方案相比有什么优劣？
2. TOCTOU 是什么？为什么点赞不用预检查？如果不用唯一索引，还有什么方式防止重复？
3. 固定窗口限流的临界问题怎么解决？滑动窗口、令牌桶、漏桶各自适合什么场景？
4. SoftJWTAuth 在什么时候有用？如果 SoftJWTAuth 下 token 过期了，应该返回 401 还是放行？
5. bcrypt 相比 MD5/SHA256 做密码哈希的优势？bcrypt 的 cost 参数设为 10 和 14 分别意味着什么？
6. HS256 和 RS256 的区别？什么场景下必须用 RS256？本项目用 HS256 有什么风险？
7. 如果限流的 Redis key 被手动删除了，会发生什么？怎么防止恶意绕过限流？
8. JWT 的 payload 是 Base64 编码而非加密，这意味着什么？能在 payload 里放什么不能放什么？

---

## 第6章：架构与工程实践

### 6.1 分层架构与事务传递

**Handler -> Service -> Repository 三层分离：**

```go
// backend_repeat/internal/http/router.go:SetRouter
// 依赖注入的组装顺序
accountRepository := account.NewAccountRepository(db)
accountService := account.NewAccountService(accountRepository, cache)
accountHandler := account.NewAccountHandler(accountService)
```

**事务传递模式：**

Service 不直接访问 `repo.db`，而是通过 `Transaction` 方法获取事务对象：

```go
// backend_repeat/internal/video/like_repo.go:Transaction
func (r *LikeRepository) Transaction(ctx context.Context, fn func(tx *gorm.DB) error) error {
    return r.db.WithContext(ctx).Transaction(fn)
}

// backend_repeat/internal/video/like_service.go:Like (降级路径)
err := s.likeRepo.Transaction(ctx, func(tx *gorm.DB) error {
    // 通过 tx 调用各 repo 的事务版本方法
    if err := s.likeRepo.LikeInTx(ctx, tx, like); err != nil {
        return err
    }
    if err := s.videoRepo.ChangeLikesCount(ctx, tx, videoID, 1); err != nil {
        return err
    }
    if err := s.videoRepo.ChangePopularity(ctx, tx, videoID, 1); err != nil {
        return err
    }
    return nil
})
```

**InTx 方法设计：**

```go
// LikeInTx -- 在已有事务中插入点赞记录
func (r *LikeRepository) LikeInTx(ctx context.Context, tx *gorm.DB, like *Like) error { ... }

// DeleteByVideoAndAccountInTx -- 在已有事务中删除点赞记录
func (r *LikeRepository) DeleteByVideoAndAccountInTx(ctx context.Context, tx *gorm.DB, videoID, accountID uint) error { ... }

// CreateCommentInTx -- 在已有事务中创建评论
func (r *CommentRepository) CreateCommentInTx(ctx context.Context, tx *gorm.DB, comment *Comment) error { ... }
```

每个 Repository 方法都提供普通版本和 `InTx` 版本。普通版本使用 `r.db`，`InTx` 版本接受外部传入的 `tx`。

### 6.2 防御性编程

**GREATEST 兜底防负数：**

```go
// backend_repeat/internal/video/video_repo.go:ChangeLikesCount
db.WithContext(ctx).Model(&Video{}).
    Where("id = ?", id).
    UpdateColumn("likes_count", gorm.Expr("GREATEST(likes_count + ?, 0)", change)).Error
```

`GREATEST(likes_count + ?, 0)` 保证即使并发扣减导致负数，也会被修正为 0。

**nil 检查：**

```go
// backend_repeat/internal/middleware/redis/redis.go:Lock
func (c *Client) Lock(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
    if c == nil || c.rdb == nil {
        return "", false, nil  // Redis 不可用时优雅降级
    }
    // ...
}
```

所有 Redis 操作方法都检查 `c == nil || c.rdb == nil`，保证 Redis 不可用时不会 panic，而是返回零值或降级。

**参数校验：**

```go
// Handler 层：binding:"required"
type ListByAuthorIDRequest struct {
    AuthorID uint `json:"author_id" binding:"required"`
}

// Service 层：手动校验
func (s *LikeService) Like(ctx context.Context, accountID, videoID uint) error {
    if accountID == 0 || videoID == 0 {
        return errors.New("video_id and account_id are required")
    }
    // ...
}
```

### 6.3 Go 并发模式

**singleflight -- 请求合并：**

```go
// backend_repeat/internal/feed/service.go:GetVideoByIDs
sfKey := fmt.Sprintf("sf:entity:%d", videoID)
v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
    return f.repo.GetByIDs(ctx, []uint{videoID})
})
```

多个 goroutine 对同一个 key 调用 `Do` 时，只有一个会执行函数体，其他 goroutine 等待并共享结果。

**WaitGroup + Mutex -- 并行扇出 + 共享 map 保护：**

```go
// backend_repeat/internal/feed/service.go:GetVideoByIDs
var wg sync.WaitGroup
var mu sync.Mutex
for _, id := range missedL2 {
    wg.Add(1)
    go func(videoID uint) {
        defer wg.Done()
        // ... 查询逻辑 ...
        if err == nil && v != nil {
            mu.Lock()
            videoMap[id] = &safeCopy  // 写共享 map 需要加锁
            mu.Unlock()
        }
    }(id)
}
wg.Wait()  // 等待所有 goroutine 完成
```

**context 超时传播：**

```go
// backend_repeat/internal/video/popularity_cache.go:UpdatePopularityCache
// Redis 操作用独立超时，避免被请求 context 取消连带
opCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
defer cancel()
_ = cache.ZincrBy(opCtx, windowKey, member, float64(change))
```

关键设计：Redis 操作使用独立的 `context.WithTimeout(context.Background(), 50ms)`，而不是传入的 `ctx`。原因：即使用户请求被取消（如客户端断开），热度更新仍应完成。

### 6.4 优雅停机

**API 进程 -- `GracefulShutdown`：**

```go
// backend_repeat/internal/http/router.go:GracefulShutdown
func GracefulShutdown(srv *http.Server) {
    stop := make(chan os.Signal, 1)
    signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
    <-stop  // 阻塞直到收到信号
    log.Printf("api server is shutting down...")

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    if err := srv.Shutdown(ctx); err != nil {
        log.Fatalf("api server shutdown failed: %v", err)
    }
    log.Printf("server exited gracefully")
}
```

**Worker 进程 -- signal.NotifyContext：**

```go
// backend_repeat/cmd/worker/main.go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

// 启动 5 个消费者 goroutine
errCh := make(chan error, 5)
go func() { errCh <- socialWorker.Run(ctx) }()
go func() { errCh <- likeWorker.Run(ctx) }()
go func() { errCh <- commentWorker.Run(ctx) }()
go func() { errCh <- popularityWorker.Run(ctx) }()
go func() { errCh <- fanoutWorker.Run(ctx) }()

err = <-errCh  // 等待任一 goroutine 退出
```

Worker 的优雅停机流程：收到 SIGTERM -> ctx 被取消 -> 各 Worker 的 `Run` 方法中 `select { case <-ctx.Done(): return ctx.Err() }` 触发 -> 当前消息处理完成后退出。

### 6.5 pprof 可观测性

```go
// backend_repeat/internal/observability/pprof.go:NewPprofMux
func NewPprofMux() *http.ServeMux {
    mux := http.NewServeMux()
    mux.HandleFunc("/debug/pprof/", pprof.Index)          // pprof 首页
    mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline) // 启动命令参数
    mux.HandleFunc("/debug/pprof/profile", pprof.Profile) // CPU profile
    mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)   // 符号信息
    mux.HandleFunc("/debug/pprof/trace", pprof.Trace)     // execution trace
    return mux
}
```

pprof 服务运行在独立端口（`cfg.ObservabilityConfig.Pprof.ApiAddr` / `WorkerAddr`），不影响主服务性能。API 和 Worker 各自启动独立的 pprof 实例。

### 6.6 Dockerfile 多阶段构建

项目使用多阶段构建优化镜像大小：

```dockerfile
# build 阶段：golang 镜像编译 api + worker
FROM golang:xxx AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o api ./cmd/main.go
RUN go build -o worker ./cmd/worker/main.go

# base 阶段：alpine 最小镜像
FROM alpine:latest AS base
RUN apk add --no-cache ca-certificates tzdata

# api target
FROM base AS api
COPY --from=build /app/api /usr/local/bin/
ENTRYPOINT ["api"]

# worker target
FROM base AS worker
COPY --from=build /app/worker /usr/local/bin/
ENTRYPOINT ["worker"]
```

**好处：**

1. 最终镜像只包含二进制文件和最小运行时依赖（alpine + ca-certificates + tzdata）
2. 编译阶段的 Go 工具链和源代码不进入最终镜像
3. `api` 和 `worker` 两个 target 可以独立构建和部署

### 自检题

1. 为什么 Service 层不直接访问 `repo.db`？如果 Service 直接用 db 会破坏什么设计原则？
2. `GREATEST` 函数的作用是什么？在什么并发场景下 likes_count 可能变成负数？
3. singleflight 和 `sync.Once` 的区别？singleflight 的结果会被缓存吗？
4. 优雅停机的流程是什么？如果有一个请求处理了 10 秒还没完成，会怎样？
5. Dockerfile 多阶段构建的好处？为什么不用 scratch 而用 alpine？
6. `context.WithTimeout(context.Background(), 50ms)` 为什么用 Background 而不是传入的 ctx？什么场景下应该用传入的 ctx？
7. WaitGroup + Mutex 的组合模式中，如果某个 goroutine panic 了，wg.Wait() 会永远阻塞吗？怎么防护？
8. 依赖注入通过构造函数实现，和 wire/dig 等 DI 框架相比有什么优劣？

---

## 第7章：可观测性、熔断与高级限流

> 性能优化的前提是"看得见"。本章从指标采集到系统韧性，覆盖项目可观测性体系的完整闭环。

### 7.1 Prometheus + Grafana 可观测性体系

**Pull 模式 vs Push 模式**

Prometheus 采用 **Pull 模式**：服务暴露 `/metrics` 端点，Prometheus 每 15s 主动抓取。这相比 Push 模式（StatsD/Graphite）的优势是：

- 服务方不需要知道监控系统在哪
- 监控系统可以发现死掉的目标（scrape 失败 → up == 0）
- 抓取频率由监控系统决定，避免过载

代码位置：`backend_repeat/cmd/main.go` 注册 `/metrics`：

```go
import "github.com/prometheus/client_golang/prometheus/promhttp"

router.GET("/metrics", gin.WrapH(promhttp.Handler()))
```

**指标类型选型**

| 类型 | 用途 | 示例 |
|------|------|------|
| Counter | 单调递增计数 | 请求总数、错误总数 |
| Gauge | 可增可减的瞬时值 | 当前连接数、队列长度 |
| Histogram | 分位数与 SLI 监控 | 请求延迟、消息大小 |
| Summary | 服务端预聚合的分位数 | 不推荐，无法跨实例聚合 |

**项目指标清单**（`backend_repeat/internal/observability/metrics.go`）：

```go
// HTTP 层（Gin middleware 自动采集）
HTTPRequestsTotal      // CounterVec, label: method, path, status_code
HTTPRequestDuration    // HistogramVec, label: method, path

// Redis 层（cache.go + zset.go + set.go 手动埋点，所有操作均有指标）
RedisOperationsTotal       // CounterVec, label: operation (get/set/del/zadd/zrevrangebyscore/zincrby/sadd/smembers 等)
RedisOperationDuration     // HistogramVec, label: operation

// MQ 层
MQMessagesPublished        // CounterVec, label: exchange, routing_key
MQMessagesConsumed         // CounterVec, label: queue

// 系统韧性（v2 新增）
CircuitBreakerStateChanges // CounterVec, label: to_state
CircuitBreakerRejections   // Counter
RateLimitRejections        // CounterVec, label: limiter, prefix
```

**Gin 中间件自动采集**（`internal/observability/middleware.go`）：

```go
func MetricsMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        start := time.Now()
        c.Next()
        // 用 c.FullPath() 而不是 c.Request.URL.Path，避免参数 explosion
        path := c.FullPath()
        if path == "" { path = "unknown" }
        HTTPRequestsTotal.WithLabelValues(
            c.Request.Method, path,
            strconv.Itoa(c.Writer.Status()),
        ).Inc()
        HTTPRequestDuration.WithLabelValues(c.Request.Method, path).
            Observe(time.Since(start).Seconds())
    }
}
```

**为什么用 `FullPath()`**：路径参数会让 path label 爆炸。例如 `/user/1`、`/user/2`、`/user/3` 都是同一接口，但如果用 `URL.Path` 就会创建 N 个 label，最终拖垮 Prometheus 内存。`FullPath()` 返回路由模板 `/user/:id`，是聚合维度。

**Histogram 桶选型**：

```go
// HTTP：覆盖 1ms 到 5s
Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

// Redis：覆盖 0.1ms 到 1s（Redis 大多数操作都在亚毫秒级）
Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1}
```

桶选错的代价：要么所有请求都落在同一个桶里（无法看出分布），要么 P95/P99 落在桶边界上失真。

**PromQL 实战**：

```promql
# QPS by path
rate(feedsystem_http_requests_total[1m])

# P95 延迟
histogram_quantile(0.95, rate(feedsystem_http_request_duration_seconds_bucket[5m]))

# 熔断器开启频率（每分钟切到 open 的次数）
rate(feedsystem_circuit_breaker_state_changes_total{to_state="open"}[1m])
```

> 注：项目当前未暴露独立的 cache_hit / cache_miss Counter，无法直接用 PromQL 计算缓存命中率。如需精确命中率，需在 `GetBytes` 中新增 `feedsystem_redis_cache_hits_total` 和 `feedsystem_redis_cache_misses_total` 两个 Counter。压测中的"~80%"是通过 `Redis GET QPS / HTTP QPS` 近似估算的。

**Grafana Dashboard 8 个面板**：

1. HTTP QPS by path
2. HTTP QPS by status code
3. HTTP P50/P95/P99
4. HTTP P95 by path（找慢接口）
5. Redis 操作 QPS
6. Redis 操作 P95 延迟
7. MQ 发布量
8. MQ 消费量

### 7.2 熔断器模式（Circuit Breaker）

**问题背景**

降级逻辑只判 `if cache != nil` 不足以应对所有场景：

| 故障类型 | 简单判 nil 能处理吗？ |
|----------|-----------------------|
| Redis 进程挂了，连不上 | 能（连接错误立即返回） |
| Redis 网络抖动，每次响应 200ms | **不能** —— 每个请求都要等 200ms 超时，连接池被打满 |
| Redis 半挂状态，部分命令成功部分失败 | **不能** —— 错误率上升但仍在响应 |

熔断器的价值：识别"系统正在变差"，主动断流保护后端，给故障组件恢复时间。

**三态机**

```
    连续 N 次失败
Closed ────────────► Open
  ▲                    │
  │ M 次成功           │ 等待 Timeout
  │                    ▼
  └──────── HalfOpen ──┘
            （任一失败立即回 Open）
```

- **Closed**：正常放行，统计失败次数
- **Open**：直接拒绝，避免雪崩
- **HalfOpen**：放行少量探测请求，根据结果决定回到 Closed 还是 Open

**为什么 HalfOpen 失败要立即回 Open**：如果探测失败说明后端还没恢复，继续放流量只会再次拖垮它。立即回 Open 让它再睡一个 Timeout 周期。

**项目实现**（`backend_repeat/internal/middleware/redis/breaker.go`）：

基于 [`sony/gobreaker/v2`](https://github.com/sony/gobreaker)，封装为 `Breaker` 类型，提供 `Execute(fn func() error) error` 接口：

```go
func (b *Breaker) Execute(fn func() error) error {
    if b == nil || b.cb == nil {
        return fn()
    }

    var origErr error
    _, cbErr := b.cb.Execute(func() (any, error) {
        origErr = fn()
        // 缓存未命中是正常业务结果，不计入熔断失败
        if origErr != nil && IsMiss(origErr) {
            return nil, nil
        }
        return nil, origErr
    })

    if errors.Is(cbErr, gobreaker.ErrOpenState) ||
       errors.Is(cbErr, gobreaker.ErrTooManyRequests) {
        observability.CircuitBreakerRejections.Inc()
        return ErrBreakerOpen
    }
    if cbErr != nil {
        return cbErr
    }
    return origErr  // 透传 redis.Nil
}
```

**关键设计点**：

1. **`redis.Nil` 不计入失败**：缓存未命中只意味着回源 MySQL，是正常业务路径
2. **状态切换上报 Prometheus**：`OnStateChange` 回调里调 `CircuitBreakerStateChanges.WithLabelValues(...)`
3. **nil-safe**：`b == nil` 时直接执行 fn，避免破坏现有 nil-check 模式
4. **全覆盖**：所有 Redis 操作（cache.go: GetBytes/SetBytes/Del/ZincrBy/Expire, zset.go: ZAdd/ZRevRange/ZRevRangeByScore 等, set.go: SAdd/SRem/SMembers）均经 breaker 保护
5. **`IsBreakerOpen()` 预检查**：Pipeline 调用方（fanoutworker、socialworker、feed/service、sliding_window）在执行前检查熔断器状态，避免在 Redis 已知不可用时阻塞等待超时。**注意**：`IsBreakerOpen` 对 `open` 和 `half_open` 状态均返回 `true`，这意味着 HalfOpen 探测期 Pipeline 路径也会跳过 Redis，可能导致过度降级。这是当前实现的一个权衡：Pipeline 无法使用 `Execute` 封装，无法参与探测
6. **Worker 退避**：outboxworker/fanoutworker 收到 `ErrBreakerOpen` 时 sleep 1s 再 Nack-requeue，避免空转热循环
7. **ListLatest MySQL 降级**：`listLatestFromDB` 在熔断器 Open 时直查 MySQL，Feed 流功能不中断

**配置参数**（`DefaultBreakerConfig`）：

```go
BreakerConfig{
    MaxRequests:         1,                // HalfOpen 放行 1 个探测
    Interval:            60 * time.Second, // Closed 状态滚动窗口
    Timeout:             10 * time.Second, // Open 持续时间
    ConsecutiveFailures: 5,                // 触发熔断的连续失败次数
}
```

**Interval 的意义**：Closed 状态下若 60s 内没有失败，计数清零。这避免"间歇性失败永远累加"的误判 —— 一周失败 5 次但分布很稀疏，不应该熔断。

### 7.3 滑动窗口限流

**固定窗口的边界突刺**

最简单的限流：`INCR key + PEXPIRE key window`，但有经典缺陷：

```
00:00 ─────────────────── 00:59 │ 01:00 ─────── 02:00
                          ╳╳╳╳╳ │ ╳╳╳╳╳
                       100 次集中 │ 100 次集中
                          这 1-2 秒内瞬时 200 次
```

设定 1 分钟限流 100 次，攻击者可以在 00:59 发 100 次，紧接着 01:00 又发 100 次 —— 2 秒内完成 200 次，是限流值的 2 倍。

**滑动窗口的解法**

把时间维度也存进 Redis：

```
key = ZSET, member = 唯一标识, score = 时间戳

每次请求：
  1. ZREMRANGEBYSCORE key '-inf' (now - window)  -- 移除过期请求
  2. count = ZCARD key
  3. if count < limit:
       ZADD key now <unique_member>
       PEXPIRE key window
       return 0  -- 允许
     else:
       return 1  -- 拒绝
```

ZSET 里始终只保留"过去 window 时间内的请求"，无论何时切片都严格不超过 limit。

**项目实现**（`backend_repeat/internal/middleware/ratelimit/sliding_window.go`）：

整段逻辑封装为 Lua 脚本，原子执行避免竞态：

```go
var slidingWindowScript = redis.NewScript(`
local key = KEYS[1]
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)
if count < limit then
    redis.call('ZADD', key, now, member)
    redis.call('PEXPIRE', key, window)
    return 0
end
return 1
`)
```

**为什么 member 用纳秒时间戳 + 原子计数器**：同一毫秒内可能有多次请求，用毫秒时间戳作 member 会被 ZADD 覆盖（因为 ZSET 同名 member 只保留最新 score），导致计数偏少。纯纳秒时间戳在高并发下也可能重复（多个 goroutine 同一纳秒），所以实际实现中追加了原子递增计数器（`fmt.Sprintf("%d:%d", nanoTs, atomicSeq)`）保证绝对唯一。

**测试用例验证不突刺**（`sliding_window_test.go:TestSlidingWindowNoBoundaryBurst`）：

```go
// 限流 3 次 / 300ms 窗口
// t=0: 发 3 次，全部 200
// t=150ms（窗口一半，旧请求仍在窗口内）: 第 4 次必须 429
time.Sleep(150 * time.Millisecond)
if sendRequest(r, "9.9.9.9") != http.StatusTooManyRequests {
    t.Fatal("at mid-window, 4th request must be rejected (no boundary burst)")
}

// 再等 160ms（总共 310ms > 300ms 窗口），旧请求被清理，配额恢复
time.Sleep(160 * time.Millisecond)
if sendRequest(r, "9.9.9.9") != http.StatusOK {
    t.Fatal("after full window elapsed, request should pass")
}
```

**与固定窗口的对比与选型**

| 维度 | 固定窗口 | 滑动窗口 |
|------|----------|----------|
| Lua 复杂度 | 1 条命令 | 4 条命令 |
| 内存 | O(1) | O(limit) |
| 单次延迟 | ~0.2ms | ~0.5ms |
| 边界突刺 | 有 | 无 |
| 适用接口 | 登录/注册（低频） | 点赞/评论（高频写） |

项目保留两套，按业务选择。

### 7.4 压测与性能优化闭环

**完整链路**：监控 → 发现瓶颈 → 假设 → 验证 → 优化 → 复测

1. **监控阶段**：Grafana "HTTP P95 by path" 面板发现 `comment/listAll` 的 P95 持续 >25ms
2. **假设阶段**：可能是 MySQL 慢查询、未加缓存、连接池打满
3. **验证阶段**：用 pprof CPU profile 看到 70% 时间在 GORM；`SHOW PROCESSLIST` 看到 MySQL 连接打满默认 2
4. **优化阶段**：加分页缓存 + 调连接池
5. **复测阶段**：QPS +246%，P95 -56%

**hey 压测工具**：

```bash
# 固定 RPS 模式（更接近真实生产流量模式）
hey -z 60s -q 100 -c 10 -m POST -H "Content-Type: application/json" \
    -d '{"video_id":1,"page":1}' \
    http://localhost:8080/comment/listAll

# 总请求数模式（找系统极限）
hey -n 50000 -c 500 ...
```

**经验**：
- 200 并发是合理的"高负载"参考点（4 核 CPU 撑得住）
- 500 并发是"极端"测试，验证有无错误率/超时
- 单接口压测 + 混合并行压测都要做（单接口可能命中缓存而不真实）

### 自检题

1. Counter 和 Gauge 的本质区别是什么？为什么 QPS 用 Counter 而不是 Gauge？如果用 Gauge 记录 QPS 会有什么问题？
2. 为什么 Prometheus 用 Pull 模式而不是 Push 模式？Pull 模式对短生命周期任务（如 cron job）有什么局限？
3. 如果 HTTP 延迟 Histogram 只设了 0.001s 和 10s 两个桶，P95 计算会有什么问题？怎么选择合适的桶边界？
4. 熔断器为什么要有 HalfOpen 状态？没有它会怎样？如果 HalfOpen 放行多个请求会有什么风险？
5. 为什么 `redis.Nil` 不应该计入熔断失败？如果计入了会怎样？（提示：缓存未命中频率）
6. 滑动窗口限流的 ZSET member 为什么不能用毫秒时间戳？高并发下纳秒时间戳就够了吗？为什么还需要原子计数器？
7. 固定窗口的"边界突刺"在什么业务场景下危害最大？对于登录接口，边界突刺的实际影响有多大？
8. 压测时 200 并发和 500 并发分别看什么指标？为什么不能只看吞吐量？
9. 如果把熔断器的 ConsecutiveFailures 设为 1 会怎样？什么场景下会误判？设为 100 呢？
10. 滑动窗口的 Lua 脚本在 Redis Cluster 模式下能直接用吗？为什么？怎么解决？
11. Prometheus 的 `rate()` 函数和 `irate()` 函数有什么区别？什么场景用哪个？
12. 熔断器的 Interval（60s 滚动窗口）是什么意思？如果去掉 Interval，一周内偶发 5 次失败就会触发熔断吗？
13. 压测时如何避免"缓存预热"导致结果虚高？冷启动压测和热启动压测分别验证什么？
14. 如果熔断器开启后所有请求直接打 MySQL，MySQL 扛得住吗？怎么防止级联雪崩？

