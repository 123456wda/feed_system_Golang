# CLAUDE.md — feedsystem_video_go 项目指南

> 版本: v3.5 | 最后更新: 2026-06-10 | 对应代码分支: main

> 当本文档与现有代码冲突时，**以现有代码为准**，但请指出冲突点。

## 0. 速查索引

| 模块 | 关键约束 |
|---|---|
| account | 密码 bcrypt、JWT 提取用户 ID、改名后使旧 token 失效 |
| video | Outbox 模式发布、删除校验所有权、缓存主动 DEL（L1+L2） |
| like | MQ 优先 + 同步降级、事务内幂等、更新 likes_count |
| comment | MQ 优先 + 同步降级、仅作者可删、更新 popularity |
| social | API 同步写库 + MQ 通知下游、SocialWorker 维护 bigv SET |
| feed | 三级缓存 + singleflight、冷热分离、k-way merge（堆大小=K）+ 大V公平采样 |
| worker | 幂等消费、手动 Ack/Nack、context 超时控制 |
| middleware/redis | 分布式锁 SETNX + Lua 解锁、所有方法判 nil、Redis 调用经 breaker 保护 |
| middleware/rabbitmq | Topic Exchange、JSON 序列化、event_id 由 `newEventID` 生成 |

## 1. 项目概览

Go 高性能短视频 Feed 流系统，API + Worker 双进程架构。Go 1.26 + Gin + GORM + MySQL 8.0 + Redis 7 + RabbitMQ 3。Docker Compose 一键部署。

## 2. 目录结构

```
feedsystem_video_go/
├── backend_repeat/              # 后端 Go 服务
│   ├── cmd/
│   │   ├── main.go              # API 进程入口 (:8080)
│   │   └── worker/main.go       # Worker 进程入口
│   ├── configs/
│   │   └── config.docker.yaml   # Docker 容器内配置
│   ├── internal/
│   │   ├── account/             # 账号模块 (handler/service/repo/entity)
│   │   ├── auth/                # JWT 签发解析 (HS256, 24h)
│   │   ├── config/              # 配置加载 (YAML + 默认值兜底)
│   │   ├── db/                  # 数据库连接 + AutoMigrate + 连接池调优
│   │   ├── feed/                # Feed 流 (三级缓存 + 推拉结合 + singleflight)
│   │   ├── http/                # Gin 路由注册 + 优雅停机
│   │   ├── middleware/
│   │   │   ├── jwt/             # 鉴权 (硬鉴权 JWTAuth + 软鉴权 SoftJWTAuth)
│   │   │   ├── rabbitmq/        # MQ 连接 + 各业务 Topic 生产者
│   │   │   ├── ratelimit/       # 限流 (固定窗口 + 滑动窗口)
│   │   │   └── redis/           # Redis 封装 (缓存/锁/ZSET/SET/限流/熔断)
│   │   ├── observability/       # Prometheus 指标 + Gin 中间件 + pprof
│   │   ├── social/              # 关注关系模块
│   │   ├── video/               # 视频/点赞/评论模块
│   │   └── worker/              # MQ 消费者 (social/like/comment/popularity/fanout/outbox)
│   ├── docs/                    # 设计文档 (push-pull-hybrid-feed.md 等)
│   ├── Dockerfile               # 多阶段构建 (api + worker)
│   └── go.mod
├── frontend/                    # 前端 Vue 3 应用 (不关心，略)
├── test/postman.json            # Postman 测试集合
├── configs/                     # 基础设施配置 (Docker Compose)
│   ├── prometheus.yml
│   └── grafana/provisioning/    # Grafana 数据源 + Dashboard 自动导入
├── docs/                        # 学习与面试文档
│   ├── backend-study-guide.md
│   ├── backend-interview-qa.md
│   ├── stress-test.md
│   ├── optimization-report.md
│   ├── bottleneck-analysis.md
│   └── observability-and-optimization.md
├── docker-compose.yml           # 8 服务: mysql/redis/rabbitmq/backend/worker/frontend/prometheus/grafana
└── study-tool.html              # 学习工具 (独立 HTML)
```

## 3. 架构与缓存

### 架构模式

分层架构 + 事件驱动（CQRS）：API 进程快速响应，Worker 异步处理。Redis/RabbitMQ 不可用时自动降级直写 MySQL。

```
客户端 → Gin → JWT 中间件 → Handler → Service → Repository → MySQL
                                    ↓
                               RabbitMQ (Topic Exchange)
                                    ↓
                         Worker 消费 → 更新 MySQL/Redis
```

### 分层规则

**纵向调用**：Handler → Service → Repository → Entity（必须遵守）

**横向调用**（合法依赖）：
- `FeedService` → `video.LikeRepository`（批量查 is_liked）
- `SocialService` → `account.AccountRepository`（查用户信息）
- `LikeService`/`CommentService` → `VideoRepository`（校验存在性、更新计数，同 package）
- `SocialWorker`/`FanoutWorker` → `social.SocialRepository`（落库 + 查粉丝）

**严禁**：Handler 跨模块调用、Service 直接调用其他 Service。

### 三级缓存体系

**L1** 本地缓存（go-cache, TTL 3s/5s）→ **L2** Redis（STRING JSON, TTL 5min 或 1h）→ **L3** MySQL

**防击穿**：
- singleflight 4 处：`GetVideoByIDs`（`sf:entity:{id}`）、`ListLatest`（`sf:fallback:feed:global_timeline_rebuild`、`sf:cold:listLatest:...`、`sf:stitch:listLatest:...`）
- 分布式锁：`GetDetail` 用 SETNX + 轮询（20ms × 5 次）
- MySQL 降级兜底：Redis 不可用时降级到 `listByFollowingFromDB`/`listLatestFromDB`

**防穿透**：`EMPTY_DB` 短路标记，查完 MySQL 为空时写短 TTL 空标记。

**L1 多实例一致性**：容忍 3-5 秒不一致，Feed 场景可接受。

### 鉴权

- **硬鉴权 `JWTAuth`**：Token 缺失/无效 → 401
- **软鉴权 `SoftJWTAuth`**：Token 缺失放行，无效 → 401。`GetAccountID(c)` 返回 error 时 `accountID == 0` 表示未登录
- JWT 显式检查 `Alg() == "HS256"` 防算法混淆。Redis 读取 50ms 超时，miss 回退 MySQL 后异步回填 Redis（自愈）
- `buildAbsoluteURL` 尊重 `X-Forwarded-Proto` header。视频文件用 `randHex(16)` 命名

## 4. API 路由

所有接口均为 **POST**，请求/响应 JSON。

**Account** (`/account`)
- `register` / `login`（限流 10/min/IP）| `changePassword`（不需 JWT）| `findByID` / `findByUsername` | `logout` / `rename`（需 JWT）

**Video** (`/video`)
- `listByAuthorID` / `getDetail`（三级缓存）| `uploadVideo`（mp4, 200MB）/ `uploadCover`（jpg/png/webp, 10MB）/ `publish`（Outbox）/ `delete`（需 JWT）

**Like** (`/like`，需 JWT，滑动窗口限流 30/min/用户)
- `like` / `unlike`（MQ 优先 + 同步降级）| `isLiked` / `listMyLikedVideos`

**Comment** (`/comment`)
- `listAll`（公开）| `publish`（需 JWT，限流 10/min）/ `delete`（需 JWT，限流 10/min，仅作者）

**Social** (`/social`，需 JWT，限流 20/min/用户)
- `follow` / `unfollow` | `getAllFollowers` / `getAllVloggers`（不传 id 查自己）

**Feed** (`/feed`)
- `listLatest` / `listLikesCount` / `listByPopularity`（软鉴权）| `listByFollowing`（需 JWT，推拉结合）

## 5. 数据模型

### MySQL 表（GORM AutoMigrate）

- **Account**: id, username(unique), password(bcrypt), token, follower_count
- **Video**: id, author_id(index), username, title, description, play_url, cover_url, create_time, likes_count, popularity
- **Like**: id, video_id+account_id(联合唯一索引), created_at
- **Comment**: id, username(index), video_id(index), author_id(index), content, created_at
- **Social**: id, follower_id+vlogger_id(联合唯一索引)
- **OutboxMsg**: id, video_id(index), author_id(index), event_type, create_time, status(pending/done)

### Redis 数据结构

| Key 模式 | 类型 | TTL | 用途 |
|----------|------|-----|------|
| `account:<id>` | STRING | 24h | 鉴权 Token（自愈：miss 回退 MySQL 后回填） |
| `video:detail:id=<id>` | STRING | 5min / 1h | 视频详情缓存，变更时 DEL |
| `feed:global_timeline` | ZSET | — | 全局时间线，score=create_time_ms，保留 1000 条 |
| `hot:video:1m:<yyyyMMddHHmm>` | ZSET | 2h | 热榜分钟窗，score=热度增量 |
| `hot:video:merge:1m:<as_of>` | ZSET | 2min | 热榜快照，ZUNIONSTORE 合并 60 窗口（decay=0.95） |
| `user_videos:<authorID>` | ZSET | 24h | 发件箱，保留 50 条 |
| `inbox:<followerID>` | ZSET | — | 收件箱，概率裁剪(1%)，上限 500 条 |
| `following:bigv:<followerID>` | SET | 24h | 用户关注的大V ID |
| `user:follower_count:<id>` | STRING | 0(永不过期⚠️) | 粉丝数缓存，判断大V用 |
| `user:active:<id>` | STRING | 72h | 登录时写入，推拉结合过滤僵尸粉 |
| `lock:<cacheKey>` | STRING | 2s | 分布式锁，SETNX + Lua 解锁 |
| `comment:list:video:{id}:page:{p}:size:{s}` | STRING | 30s | 评论分页缓存，写操作 DEL 第 1 页 |
| `ratelimit:<biz>:<key>` | STRING | 固定窗口 | INCR+PEXPIRE（登录/注册） |
| `ratelimit:sw:<biz>:<key>` | ZSET | 滑动窗口 | ZSET+Lua（点赞/评论/关注） |

### MQ 消息体

共用字段：`event_id`(string), `occurred_at`(RFC3339)

| 事件 | Routing Key | 额外字段 |
|------|-------------|----------|
| LikeEvent | `like.like` / `like.unlike` | `action`, `user_id`, `video_id` |
| CommentEvent | `comment.publish` / `comment.delete` | `action`, `comment_id?`, `username?`, `video_id?`, `author_id?`, `content?` |
| SocialEvent | `social.follow` / `social.unfollow` | `action`, `follower_id`, `vlogger_id` |
| PopularityEvent | `video.popularity.update` | `video_id`, `change`(int64) |
| TimelineEvent | `video.timeline.publish` | `video_id`, `create_time`(int64 ms) |
| FanoutEvent | `video.timeline.fanout` | `video_id`, `author_id`, `create_time`(int64 ms) |

### 配置

配置优先级：`-config` flag > `CONFIG_PATH` 环境变量 > 默认路径 `configs/config.yaml`。文件不存在时用 `DefaultLocalConfig()` 兜底。

环境变量：`CONFIG_PATH`（配置文件路径）、`JWT_SECRET`（生产必需，默认 `"feedsystem_secret"`）

DefaultLocalConfig：MySQL `localhost:3306`（root/123456/feedsystem）、Redis `localhost:6379`（123456）、RabbitMQ `localhost:5672`（admin/password123）、Pprof API `:6060` Worker `:6061`

HTTP 服务器：ReadTimeout 5s、WriteTimeout 10s、GracefulShutdown 5s、SetTrustedProxies(nil)

MySQL 连接池：MaxOpenConns=100、MaxIdleConns=25、ConnMaxLifetime=5min、ConnMaxIdleTime=3min

## 6. MQ 拓扑与 Worker

### Exchange/Queue

| Exchange | Queue | Binding Key | Consumer |
|----------|-------|-------------|----------|
| `social.events` | `social.events` | `social.*` | SocialWorker |
| `like.events` | `like.events` | `like.*` | LikeWorker |
| `comment.events` | `comment.events` | `comment.*` | CommentWorker |
| `video.popularity.events` | `video.popularity.events` | `video.popularity.*` | PopularityWorker |
| `video.timeline.events` | `video.timeline.update.queue` | `video.timeline.publish` | Consumer (API 进程内) |
| `video.timeline.events` | `video.timeline.fanout.queue` | `video.timeline.fanout` | FanoutWorker |

### MQ 写入模式差异

| 模块 | 模式 | 原因 |
|------|------|------|
| Like/Comment | MQ 优先 + 同步降级 | 高频写，MQ 成功即返回，失败降级 MySQL 事务 |
| Social | DB 优先 + MQ fire-and-forget | 强一致立即可见，MQ 仅通知下游，**MQ 失败静默忽略** |
| Video Publish | Outbox 模式 | 事务写 Video+OutboxMsg，Poller 轮询投递 TimelineMQ+FanoutMQ |

### 进程归属

| 组件 | 进程 | 说明 |
|------|------|------|
| OutboxPoller | API | `router.go` SetRouter 内启动，轮询 outbox 表（间隔 1s，批量 100 条） |
| Timeline Consumer | API | `router.go` SetRouter 内启动，写 `feed:global_timeline` |
| SocialWorker / LikeWorker / CommentWorker / PopularityWorker / FanoutWorker | Worker | `cmd/worker/main.go` 启动 |

Worker 不执行 AutoMigrate（只有 API 进程执行，避免并发 DDL 竞争）。

QoS：`prefetchCount=50, prefetchSize=0, prefetchGlobal=false`

### Worker Ack/Nack

- 成功：`msg.Ack(false)` | 可重试：`msg.Nack(false, true)` | 不可重试：`msg.Nack(false, false)`

## 7. 推拉结合 Feed

### 核心逻辑

- **普通用户**发视频 → FanoutWorker fanout 到**活跃**粉丝 `inbox:{followerID}`（推路径）
- **大V**（粉丝 ≥ `BigVThreshold`=10000）发视频 → 只写 `user_videos:{authorID}`（拉路径）
- 只推给 3 天内登录过的粉丝（`user:active:{id}` TTL 72h），僵尸粉不推
- SocialWorker 维护 `following:bigv:{followerID}` SET

### 读路径 (ListByFollowing)

并行读 inbox + 大V列表 → Pipeline 探测大V活跃度 → 按 newestAt 降序分配拉取配额 → k-way merge（堆大小=K，O(N log K)）

- 公平提前终止：所有大V都比 inbox 最老才跳过拉路径
- 跳过冷大V：`newestAt <= inboxOldest` 的不分配配额
- 每个大V最多拉取 10 条

### 关键常量

| 常量 | 值 | 文件 |
|------|-----|------|
| `BigVThreshold` | 10000 | `fanoutworker.go` |
| `inboxCap` | 500 | `fanoutworker.go` |
| `userVideosCap` | 50 | `fanoutworker.go` |
| `fanoutBatchSize` | 100 | `fanoutworker.go` |
| `trimChance` | 0.01 | `fanoutworker.go`（概率裁剪，参考 Stream-Framework） |
| 收件箱回填数 | 50 | `socialworker.go`（关注非大V时回填） |
| 大V拉取配额 | 10/大V | `feed/service.go` |

### 热榜

按分钟分桶写 ZSET，查询时 ZUNIONSTORE 按时间衰减加权合并 60 窗口（decay=0.95）。

设计参考：Stream-Framework、Twitter "Timelines at Scale"、微博 Feed 架构。详见 `backend_repeat/docs/push-pull-hybrid-feed.md`。

## 8. 超时与限流

### 超时常量

| 场景 | 超时 | 文件 |
|------|------|------|
| ListByFollowing Redis 路径 | 200ms | `feed/service.go`（超时降级 `listByFollowingFromDB`） |
| ListByPopularity 热榜合并 | 80ms | `feed/service.go` |
| ListLatest ZSET 重建 | 2000ms | `feed/service.go`（singleflight 回填 1000 条） |
| FeedService L2 Redis MGet | 50ms | `feed/service.go` |
| VideoService 分布式锁 | 50ms | `video_service.go`（超时降级 MySQL） |
| VideoService 锁轮询 | 5×20ms | `video_service.go` |
| SocialWorker onFollow | 500ms | `socialworker.go` |
| SocialWorker onUnfollow | 200ms | `socialworker.go` |
| 所有独立 Redis 操作 | 50ms | 各 service |

### 限流

| 接口 | 方式 | 窗口 | 限值 | Key |
|------|------|------|------|-----|
| login/register | 固定窗口 INCR+PEXPIRE | 1min | 10 次 | IP |
| like/unlike | 滑动窗口 ZSET+Lua | 1min | 30 次 | accountID |
| comment publish/delete | 滑动窗口 ZSET+Lua | 1min | 10 次 | accountID |
| social follow/unfollow | 滑动窗口 ZSET+Lua | 1min | 20 次 | accountID |

Redis 不可用或熔断器 Open 时 fail-open（放行）。

### 熔断器

`sony/gobreaker v2`：MaxRequests=1, Interval=60s, Timeout=10s, ConsecutiveFailures=5

- `redis.Nil` 报告为成功（不计入熔断），但原值返回调用方
- `IsBreakerOpen` 对 open/half_open 均返回 true
- `ErrBreakerOpen` 触发 MySQL 降级路径
- Worker 检测到 `ErrBreakerOpen` → sleep 1s 后 Nack+requeue（避免热循环）

## 9. 可观测性

### 指标

| 指标 | 类型 | 标签 | 采集方式 |
|------|------|------|----------|
| `feedsystem_http_requests_total` | Counter | method, path, status_code | Gin middleware 自动 |
| `feedsystem_http_request_duration_seconds` | Histogram | method, path | Gin middleware 自动 |
| `feedsystem_redis_operations_total` | Counter | operation, status | cache/zset/set 手动 |
| `feedsystem_redis_operation_duration_seconds` | Histogram | operation, status | cache/zset/set 手动 |
| `feedsystem_mq_messages_published_total` | Counter | exchange, routing_key | PublishJSON 手动 |
| `feedsystem_mq_messages_consumed_total` | Counter | queue | Worker IncrConsumed |
| `feedsystem_circuit_breaker_state_changes_total` | Counter | to_state | breaker OnStateChange |
| `feedsystem_circuit_breaker_rejections_total` | Counter | — | breaker Execute |
| `feedsystem_ratelimit_rejections_total` | Counter | limiter, prefix | ratelimit 中间件 |

采集细节：MetricsMiddleware 排除 `/metrics`，用 `FullPath()` 防标签基数爆炸。HTTP buckets 从 1ms 起，Redis 从 0.1ms 起。缓存命中率通过 `Redis GET QPS / HTTP QPS` 估算（~80%，无专用计数器）。

新增指标：`metrics.go` 定义 → `init()` MustRegister → 业务代码调用 `.Inc()`/`.Observe()`。

### 监控地址

| 服务 | 地址 | 说明 |
|------|------|------|
| Grafana | http://localhost:3001 | admin/admin123，Dashboard "FeedSystem Monitoring" |
| Prometheus | http://localhost:9090 | Targets 确认 `feedsystem-api` UP |
| /metrics | http://localhost:8080/metrics | 浏览器可直接访问 |
| RabbitMQ | http://localhost:15672 | admin/password123 |

### 压测核心数据

| 场景 | 吞吐量 | P50 | P95 | P99 |
|------|--------|-----|-----|-----|
| comment/listAll 200c | 20,220 req/s | 4.9ms | 12.7ms | 34.5ms |
| video/getDetail 200c | 24,546 req/s | 2.9ms | 6.7ms | 39.7ms |
| comment/listAll 500c | 14,422 req/s | 10.3ms | 35.1ms | 89.1ms |

优化项：评论分页缓存 → 吞吐 +246%、P95 -56%；MySQL 连接池调优 → P95 -96%。详见 `docs/stress-test.md`、`docs/optimization-report.md`。

## 10. 技术要点

### TOCTOU 竞态

- **点赞**：不预检查，直接 INSERT，唯一索引兜底 catch 1062（高频，预检查不值得）
- **关注**：保留预检查 IsFollowed，给友好提示，uniqueIndex 兜底（低频，UX 优先）

### 其他要点

- **bcrypt**：cost=10 ~100ms，仅影响 login/register，其他接口不受影响
- **Redis Cluster**：3 个 Lua 脚本均单 key，天然兼容。`ZUNIONSTORE` 合并热榜需 Hash Tag
- **Comment fallback 事务内二次校验**：`CommentService.Publish` 的 fallback 路径事务内重新检查视频存在性，防竞态
- **SocialWorker inbox 回填**：关注非大V时回填最近 50 条视频到 inbox，大V不走回填
- **UpdatePopularityCache 双重 Context**：`cache.Del()` 用 `Background()`（必须完成），`ZincrBy` 用 50ms 超时（尽力而为）

### Docker Compose

```bash
docker compose up -d --build          # 启动全部
docker compose up -d mysql redis rabbitmq  # 仅启动依赖
docker compose logs -f backend worker       # 查看日志
docker compose down                        # 停止
```

### 后端开发命令

```bash
cd backend_repeat && go run ./cmd                    # API 进程
cd backend_repeat && go run ./cmd/worker              # Worker 进程
cd backend_repeat && go build -o api ./cmd            # 编译 API
cd backend_repeat && go build -o worker ./cmd/worker  # 编译 Worker
cd backend_repeat && go test ./...                    # 运行测试
```

## 11. AI 辅助约定

### 元规则

- 代码与文档冲突时**以代码为准**，但指出冲突点
- 修改前先读目标文件最新内容，不要仅依赖本文档
- 涉及多模块时按 Entity → Repository → Service → Handler 顺序读取
- 不确定函数/常量是否存在时先 Grep 搜索，不猜测

### 硬性约束

- 所有 API 用 **POST** 方法
- 用户 ID **仅从 JWT 提取**（`GetAccountID(c)`），不信任客户端传参
- 密码 **bcrypt** 存储
- Redis/RabbitMQ 均**可选**，不可用时降级 MySQL
- GORM **AutoMigrate**，不手写 DDL
- 视频发布用 **Outbox 模式**
- 大V判断读 `fanoutworker.go` 的 `BigVThreshold`，严禁硬编码

### 代码规范

- 分层一致性：Handler → Service → Repository，不跨层
- MQ 降级：`if mq != nil { publish } else { fallback }`
- 缓存失效：数据变更时主动 DEL L1+L2 缓存
- 上下文传播：所有 DB/Redis 操作传 `context.Context`
- JSON 响应：`gin.H{"key": value}`
- 幂等设计：Worker 事务内 LikeIgnoreDuplicateInTx / ON DUPLICATE KEY
- 日志：`log.Printf`，Service 层禁止 `log.Fatal`/`os.Exit`
- 错误包装：`fmt.Errorf("...: %w", err)` 保持错误链

### Anti-patterns

```
// ❌ Handler 直接调用 Repository（跨层）
h.repo.GetDetail(ctx, id)
// ✅ Handler → Service → Repository
h.service.GetDetail(ctx, id)
```

```
// ❌ 信任客户端 user_id
service.DoSomething(req.UserID)
// ✅ 从 JWT 提取
accountID, _ := jwt.GetAccountID(c)
service.DoSomething(accountID)
```

```
// ❌ MQ 失败就丢弃
if err := mq.Publish(ctx, event); err != nil { return err }
// ✅ 降级直写
if mq != nil { if err := mq.Publish(ctx, event); err != nil { repo.Fallback() } } else { repo.Fallback() }
```

```
// ❌ 修改数据后不清理缓存
s.repo.DeleteVideo(ctx, id)
// ✅ 删除后 DEL L1+L2
s.repo.DeleteVideo(ctx, id); s.cache.Del(ctx, key); s.localcache.Delete(key)
```

```
// ❌ for 循环内 defer cancel()（context 泄漏）
for msg := range msgs { ctx, cancel := context.WithTimeout(...); defer cancel(); ... }
// ✅ 匿名函数
for msg := range msgs { func() { ctx, cancel := context.WithTimeout(...); defer cancel(); ... }() }
```

```
// ❌ Redis 操作不判 nil（降级场景 panic）
s.cache.GetBytes(ctx, key)
// ✅ 先判 nil
if s.cache != nil { s.cache.GetBytes(ctx, key) }
```

### 已知技术债

- `outboxworker.go` StartConsumer `defer cancel()` 在 for 循环内，context 泄漏
- Worker 进程 Redis/RabbitMQ 连接失败 log.Fatalf 退出，API 进程降级为 nil
- 所有 MQ 生产者/消费者共享同一 Channel（非线程安全，当前低并发可接受）
- 无 RabbitMQ 自动重连、无 Publisher Confirms
- `StartOutboxPoller` 无 `ctx.Done()` 退出检查
- `user:follower_count:<id>` TTL=0 永不过期，事件丢失会导致永久脏数据
- 评论缓存失效只 DEL `page:1:size:20`，其他组合靠 TTL 自然过期
- `commentworker.go` applyPublish 传 `tx=nil` 给 `ChangePopularity`，评论插入和热度更新不在同一事务
- `/account/login` 密码错误返回 500 而非 401
- `/video/listByAuthorID` 无 LIMIT
- Redis nil-safe 不一致：`cache.go` 静默返回零值，`set.go`/`zset.go` 返回显式 error
- 测试覆盖率低：仅 breaker/redis/ratelimit/pprof/likeworker/feed 有测试

### 测试约定

- 新增核心 Service 逻辑时主动生成 `_test.go`，放同目录
- Go 标准 `testing` + `testify`，Repo 层可用真实 MySQL
