# CLAUDE.md — feedsystem_video_go 项目指南

> 版本: v3.4 | 最后更新: 2026-06-08 | 对应代码分支: main

## 0. AI 速查索引

> 当本文档与现有代码冲突时，**以现有代码为准**，但请指出冲突点。

| 你要修改的模块 | 重点阅读 | 必须遵守的约束 |
|---|---|---|
| account | 第 4 节 Account 接口、第 5 节 Account 表 | 密码 bcrypt、JWT 提取用户 ID、改名后使旧 token 失效 |
| video | 第 3 节三级缓存、第 4 节 Video 接口 | 发布用 Outbox 模式、删除需校验所有权、缓存主动 DEL |
| like | 第 4 节 Like 接口、第 8 节 MQ 拓扑 | MQ 优先 + 同步降级、事务内幂等（LikeIgnoreDuplicateInTx）、更新 likes_count |
| comment | 第 4 节 Comment 接口、第 8 节 MQ 拓扑 | MQ 优先 + 同步降级、仅作者可删、更新 popularity |
| social | 第 4 节 Social 接口、第 8 节推拉结合 | API 同步写库 + MQ 通知下游、SocialWorker 维护 bigv SET |
| feed | 第 3 节缓存架构、第 8 节推拉结合 | 三级缓存 + singleflight、冷热分离、真正的 k-way merge（堆大小=K）+ 大V公平采样 |
| worker | 第 8 节 MQ 拓扑 + 消息体结构 | 幂等消费、手动 Ack/Nack、context 超时控制 |
| middleware/redis | 第 5 节 Redis 数据结构 | 分布式锁用 SETNX + Lua 解锁、所有方法需判 nil、所有 Redis 调用需经 breaker 保护 |
| middleware/rabbitmq | 第 8 节 MQ 拓扑 + 消息体结构 | Topic Exchange、JSON 序列化、event_id 由 `newEventID` 生成 |
| observability | 第 9 节可观测性 | 新增指标需注册到 `init()`、middleware 自动采集 HTTP 指标、Redis/MQ 手动埋点 |
| 前端 | 第 4 节前端路由、第 6 节前端规范 | Composition API + `<script setup>`、TypeScript 严格模式、API 调用走 `client.ts` 的 `postJson`/`postForm` |

**高频问题速查**：

| 问题 | 定位 |
|---|---|
| 如何新增一个 API 接口？ | 第 4 节（路由注册）+ 第 6 节（分层命名）+ 第 10 节（Anti-patterns） |
| 缓存 key 的 TTL 是多少？ | 第 5 节 Redis 数据结构表 |
| MQ 消息发到哪个 Exchange？ | 第 8 节 MQ 拓扑表 |
| 推拉结合怎么运作的？ | 第 8 节"推拉结合 Feed" + `backend_repeat/docs/push-pull-hybrid-feed.md` + `internal/feed/service.go` 的 ListByFollowing |
| 热榜怎么计算的？ | 第 8 节"热榜滑动窗口" + `internal/video/popularity_cache.go` |
| 怎么判断用户是不是大V？ | `internal/worker/fanoutworker.go` 的 `BigVThreshold`（10000 粉丝） |
| 软鉴权和硬鉴权的区别？ | 第 4 节"鉴权中间件说明" |
| 前端怎么传 JWT？ | 第 6 节"前端 Token 传递" |
| 怎么新增一个监控指标？ | 第 9 节可观测性 + `internal/observability/metrics.go` |
| Grafana Dashboard 在哪看？ | `http://localhost:3001`（admin / admin123） |
| Prometheus 在哪看？ | `http://localhost:9090` |
| 项目整体流程图在哪？ | `picture/整体流程.png` + `picture/整体架构.png` + `feedsystem_video_go项目设计.md` |
| Postman 测试集合在哪？ | `test/postman.json`（预置变量、批量跑接口与断言脚本） |
| 面试题在哪？ | `docs/backend-interview-qa.md`（10 维度 60+ 题，含速答表和代码关联） |
| 可观测性+优化面试话术在哪？ | `docs/observability-and-optimization.md`（合并版，含面试叙事线） |
| 学习工具在哪？ | `study-tool.html`（根目录，独立 HTML 应用，纯原生 JS，无依赖）+ `frontend/study.html`（前端目录下，含 Google Fonts 外部依赖） |

## 1. 项目概览

本项目是一款基于 Go 的高性能短视频 Feed 流系统，提供账号、视频、点赞、评论、关注与 Feed 流等完整功能，通过 Redis 缓存和 RabbitMQ 异步消息队列优化性能，支持 Docker Compose 一键部署。项目采用 API + Worker 双进程架构，适用于短视频社交平台的后端服务场景。

- **领域**: Web 后端服务（短视频 Feed 流系统）
- **主要技术栈**: Go 1.26 + Gin + GORM + MySQL 8.0 + Redis 7 + RabbitMQ 3
- **前端**: Vue 3 + TypeScript + Vite + Pinia + Vue Router
- **可观测性**: Prometheus（指标采集）+ Grafana（可视化 Dashboard）
- **容器化**: Docker / Docker Compose
- **项目演示**: [Bilibili 视频演示](https://www.bilibili.com/video/BV1Dti7B9E6Y)

## 2. 项目目录结构

```
feedsystem_video_go/
├── backend_repeat/                  # 后端 Go 服务（核心业务）
│   ├── cmd/
│   │   ├── main.go                  # API 进程入口
│   │   └── worker/
│   │       └── main.go              # Worker 进程入口（异步消费 MQ）
│   ├── configs/
│   │   ├── config.yaml              # 本地开发配置（MySQL 3306）
│   │   ├── config.docker.yaml       # Docker 容器内配置
│   │   └── config.compose-local.yaml # 本地进程 + Compose 依赖配置（MySQL 3307）
│   ├── internal/
│   │   ├── account/                  # 用户账号模块（handler/service/repo/entity）
│   │   ├── auth/                     # JWT 签发与解析（HS256，24h 过期，可配 JWT_SECRET）
│   │   ├── config/                   # 配置加载（YAML 解析 + 默认值兜底）
│   │   ├── db/                       # 数据库连接、AutoMigrate、连接池调优（MaxOpen=100 MaxIdle=25）
│   │   ├── feed/                     # Feed 流模块（三级缓存 + 推拉结合 + singleflight）
│   │   ├── http/                     # Gin 路由注册、服务启动与优雅停机
│   │   ├── middleware/
│   │   │   ├── jwt/                  # JWT 鉴权中间件（硬鉴权 JWTAuth + 软鉴权 SoftJWTAuth）
│   │   │   ├── rabbitmq/            # RabbitMQ 连接与各业务 Topic 生产者
│   │   │   ├── ratelimit/           # 基于 Redis 的限流（固定窗口 + 滑动窗口）
│   │   │   │   ├── ratelimit.go         # 固定窗口限流（INCR+PEXPIRE，登录/注册）
│   │   │   │   ├── sliding_window.go    # 滑动窗口限流（ZSET+Lua，点赞/评论/关注）
│   │   │   │   └── sliding_window_test.go # 滑动窗口测试（5 个用例）
│   │   │   └── redis/               # Redis 客户端封装（缓存/锁/ZSET/SET/限流/熔断）
│   │   │       ├── redis.go         # 连接管理 + 分布式锁（Lock/UnLock）+ 限流（IncrementWithExpire）
│   │   │       ├── breaker.go       # 熔断器封装（sony/gobreaker v2，保护所有 Redis 调用）
│   │   │       ├── breaker_test.go  # 熔断器单元测试（6 个用例）
│   │   │       ├── cache.go         # 基础读写（SetBytes/GetBytes/Del）+ ZincrBy + Expire
│   │   │       ├── zset.go          # ZSET 批量操作 + MGet/Exists/GetRedisClient
│   │   │       └── set.go           # 集合操作（SAdd/SRem/SMembers）
│   │   ├── observability/           # 可观测性模块
│   │   │   ├── pprof.go             # pprof 性能分析服务（API :6062 / Worker :6061）
│   │   │   ├── metrics.go           # Prometheus 指标定义（HTTP/Redis/MQ）+ 注册
│   │   │   └── middleware.go        # Gin metrics 中间件（自动采集 QPS + 延迟）
│   │   ├── social/                   # 关注关系模块
│   │   ├── video/                    # 视频/点赞/评论模块
│   │   └── worker/                   # MQ 消费者
│   │       ├── socialworker.go      # SocialWorker：落库 + bigv SET 维护 + inbox 回填
│   │       ├── likeworker.go        # LikeWorker：事务内幂等落库 + 更新 likes_count
│   │       ├── commentworker.go     # CommentWorker：幂等落库 + 更新 popularity
│   │       ├── popularityworker.go  # PopularityWorker：更新 Redis 热榜分钟窗
│   │       ├── fanoutworker.go      # FanoutWorker：推拉结合推路径
│   │       └── outboxworker.go      # Outbox 轮询器 + Timeline 消费者
│   ├── docs/                         # 设计文档
│   ├── Dockerfile                    # 多阶段构建（api + worker 两个 target）
│   ├── go.mod / go.sum               # Go 依赖管理
│   └── cmd.exe / worker.exe          # 预编译二进制（开发用）
├── frontend/                         # 前端 Vue 3 应用
│   ├── src/
│   │   ├── api/                      # 后端 API 调用封装
│   │   ├── components/               # 通用组件（AppShell/FeedVideoCard/Toaster/UserAvatar/JsonBox）
│   │   ├── router/                   # Vue Router 路由定义
│   │   ├── stores/                   # Pinia 状态管理（auth/social/toast）
│   │   ├── utils/                    # 工具函数（JWT payload 解码）
│   │   └── views/                    # 页面视图
│   ├── vite.config.ts                # Vite 配置（代理 /api → 127.0.0.1:8080）
│   ├── package.json                  # 前端依赖
│   ├── nginx.conf                    # 生产部署 nginx 配置
│   ├── Dockerfile                    # 前端容器化
│   └── study.html                    # 前端学习工具（独立 HTML，含交互式测验）
├── test/                             # Postman 测试集合
│   └── postman.json                  # 预置变量、批量跑接口与部分断言脚本
├── picture/                          # 设计文档图片（表关系、流程图、架构图等 12 张）
├── data/                             # RabbitMQ 数据目录
├── configs/                          # 基础设施配置（Docker Compose 使用）
│   ├── prometheus.yml                # Prometheus 抓取配置
│   └── grafana/                      # Grafana provisioning 配置
│       └── provisioning/
│           ├── datasources/
│           │   └── prometheus.yml    # 数据源定义
│           ├── dashboards/
│           │   ├── dashboards.yml    # Dashboard 加载配置
│           │   └── json/
│           │       └── feedsystem.json  # Dashboard 定义（8 面板）
├── docker-compose.yml                # 一键编排（含 Prometheus + Grafana）
├── start.sh                          # 本地一键启动脚本
├── README.md                         # 项目说明
└── feedsystem_video_go项目设计.md     # 详细设计文档

docs/                                 # 学习与面试文档（根目录）
├── backend-study-guide.md            # 技术学习文档（7 章，含代码引用和自检题）
├── backend-interview-qa.md           # 模拟面试 Q&A（10 维度 60+ 题，含速答表）
└── observability-and-optimization.md # 可观测性、压测与性能优化报告（含面试话术）

study-tool.html                       # 学习工具（根目录，独立 HTML 应用）
```

## 3. 核心模块与架构

### 架构模式

**分层架构 + 事件驱动（CQRS 思路）**

- **API 进程**: 接收 HTTP 请求 → 鉴权/参数校验 → 同步写核心数据 → 发布 MQ 事件 → 快速返回
- **Worker 进程**: 消费 MQ 事件 → 异步落库/更新计数/更新热榜/失效缓存
- **降级策略**: Redis/RabbitMQ 不可用时自动降级为直写 MySQL，保证核心功能可用

### 数据流

```
客户端 → Gin Router → JWT 中间件 → Handler → Service → Repository → MySQL
                                      ↓
                                 RabbitMQ (Topic Exchange)
                                      ↓
                           Worker 消费 → 更新 MySQL/Redis
```

### 核心模块列表

| 模块 | 职责 | 关键文件 | 依赖 |
|------|------|----------|------|
| **account** | 注册/登录/改密/改名/登出 | `internal/account/{handler,service,repo,entity}.go` | MySQL, Redis(可选) |
| **video** | 视频发布/删除/详情/上传（三级缓存 + 分布式锁防击穿） | `internal/video/video_{handler,service,repo,entity}.go` | MySQL, Redis, PopularityMQ, go-cache(L1) |
| **like** | 点赞/取消点赞/查询（MQ 优先 + 同步降级） | `internal/video/like_{handler,service,repo,entity}.go` | MySQL, Redis(可选), LikeMQ, PopularityMQ |
| **comment** | 评论发布/删除/列表（MQ 优先 + 同步降级 + Redis 分页缓存） | `internal/video/comment_{handler,service,repo,entity}.go` | MySQL, Redis(分页缓存 TTL 30s), CommentMQ, PopularityMQ |
| **social** | 关注/取关/粉丝列表/关注列表 | `internal/social/{handler,service,repo,entity}.go` | MySQL, SocialMQ, AccountRepo |
| **feed** | 多策略 Feed 流（三级缓存 + 推拉结合 + singleflight） | `internal/feed/{handler,service,repo,entity}.go` | MySQL, Redis, go-cache(L1), LikeRepo |
| **worker** | MQ 消费者（落库/计数/热榜/fanout/bigv SET） | `internal/worker/*.go` | RabbitMQ, MySQL, Redis |
| **middleware/jwt** | JWT 鉴权（硬鉴权 + 软鉴权） | `internal/middleware/jwt/jwt.go` | account repo, Redis |
| **middleware/redis** | Redis 封装（缓存/锁/ZSET/SET/限流/熔断） | `internal/middleware/redis/*.go` | Redis, sony/gobreaker |
| **middleware/rabbitmq** | RabbitMQ 连接与 Topic 生产者 | `internal/middleware/rabbitmq/*.go` | RabbitMQ |
| **middleware/ratelimit** | 固定窗口限流 + 滑动窗口限流 | `internal/middleware/ratelimit/{ratelimit,sliding_window}.go` | Redis |
| **observability** | Prometheus 指标 + Gin 中间件 + pprof | `internal/observability/{metrics,middleware,pprof}.go` | prometheus/client_golang |

### 分层设计与横向调用规则

**纵向调用**（必须遵守）：Handler → Service → Repository → Entity

**横向调用**（已有的合法依赖）：
- `FeedService` → `video.LikeRepository`（批量查 is_liked 状态）
- `SocialService` → `account.AccountRepository`（查用户信息用于粉丝/关注列表）
- `LikeService` / `CommentService` → `VideoRepository`（校验视频存在性、更新计数/热度，同 package 内调用）
- `SocialWorker` → `social.SocialRepository`（落库 + 查粉丝列表）
- `FanoutWorker` → `social.SocialRepository`（查粉丝 ID 列表）

**规则**：Service 层可横向调用其他模块的 Repository（通过依赖注入），但 **严禁** Handler 层跨模块调用、或 Service 层直接调用其他 Service。

### 缓存架构

**三级缓存体系**（VideoService.GetDetail / FeedService.GetVideoByIDs）：
- **L1 本地缓存**: `go-cache`（内存，默认 TTL 3s，回写时 5s），命中率最高的热数据
- **L2 Redis 缓存**: STRING 类型 JSON，TTL **5min**（`VideoService.GetDetail` 写入，因视频详情变化频率较高）/ **1h**（`FeedService.GetVideoByIDs` 写入，Feed 场景容忍更长一致性窗口）
- **L3 MySQL**: 最终数据源，回源后异步回写 L2 + 同步写 L1

**防击穿机制**：
- **singleflight**: `GetVideoByIDs`（L3 回源，key `"sf:entity:{id}"`）、`ListLatest`（ZSET 重建 key `"sf:fallback:feed:global_timeline_rebuild"`、冷查询 key `"sf:cold:listLatest:..."`、冷热衔接补尾 key `"sf:stitch:listLatest:..."`），共 4 处
- **分布式锁**: `GetDetail` 用 SETNX + 轮询等待（20ms × 5 次）
- 单机部署下两者效果一样，用两种技术是为了丰富技术栈
- **MySQL 降级兜底**: Redis 不可用时自动降级到 MySQL 查询（`ListByFollowing` 的 `listByFollowingFromDB`）

**防穿透机制**：
- **EMPTY_DB 短路标记**: `ListLatest` 中查完 MySQL 发现为空时，写入一个短 TTL 的空标记到 Redis，后续请求直接返回空，避免反复穿透到 DB。如果对应数据后来被创建，下次查询会正常回源并覆盖标记

**多实例 L1 缓存一致性**：
- L1 本地缓存是进程内的，多实例之间不共享。实例 A 更新数据 DEL Redis 后，实例 B 的 L1 在 TTL（3-5s）内仍返回旧数据
- 当前策略：容忍 3-5 秒不一致，Feed 流场景可接受，用户刷新即可看到最新数据
- 如需更强一致性：可引入 Redis Pub/Sub 广播失效消息，或缩短 L1 TTL 到 1s（代价是 Redis QPS 上升）

### 鉴权中间件说明

- **硬鉴权 `JWTAuth`**: Token 缺失或无效时直接返回 401，用于需要登录的接口
- **软鉴权 `SoftJWTAuth`**: Token 缺失时放行（`c.Next()`），Token 无效时返回 401。用于公共接口（如 feed/listLatest），未登录也能访问，但登录用户能拿到个性化数据（如 is_liked）。**注意**：软鉴权下 `GetAccountID(c)` 返回 error，需判断 `accountID == 0` 表示未登录
- **JWT 安全细节**：`auth.ParseToken` 显式检查 `token.Method.Alg() == "HS256"`，防止算法混淆攻击（如将 RS256 降级为 none）。`jwtSecret()` 使用 `sync.Once` 确保默认密钥警告只输出一次
- **Token 验证 Redis 超时**：JWT 中间件的 Redis 读取使用 **50ms 超时**（`context.WithTimeout`），优先延迟而非一致性。Redis miss 时回退 MySQL，MySQL 成功后异步回填 Redis（24h TTL）—— 自愈机制
- **静态文件服务**：`router.go` 中 `r.Static("/static", "./.run/uploads")` 提供视频/封面文件的 HTTP 访问
- **视频文件命名**：上传时使用 `randHex(16)` 生成 32 字符随机 hex 文件名，避免冲突并隐藏原始文件名（`video_handler.go`）
- **`buildAbsoluteURL`**：生成视频/封面的绝对 URL，尊重 `X-Forwarded-Proto` header（反向代理场景），`SetTrustedProxies(nil)` 仅影响 `c.ClientIP()`，不影响此函数（`video_handler.go:277-286`）。核心逻辑：
  ```go
  func buildAbsoluteURL(c *gin.Context, p string) string {
      scheme := "http"
      if c.Request.TLS != nil { scheme = "https" }
      if forwardedProto := c.GetHeader("X-Forwarded-Proto"); forwardedProto != "" {
          scheme = forwardedProto  // 反向代理（如 nginx）传入的协议
      }
      return fmt.Sprintf("%s://%s%s", scheme, c.Request.Host, p)
  }
  ```

## 4. 关键接口与入口点

### 程序入口

| 入口 | 路径 | 说明 |
|------|------|------|
| API 进程 | `backend_repeat/cmd/main.go` | HTTP 服务，监听 `:8080` |
| Worker 进程 | `backend_repeat/cmd/worker/main.go` | MQ 消费者，异步处理业务事件 |

### API 路由分组

所有接口均为 **POST** 方法，请求/响应均为 JSON。

**Account 模块** (`/account`)
- `/account/register` — 注册（限流 10/min/IP）
- `/account/login` — 登录（限流 10/min/IP）
- `/account/changePassword` — 修改密码（**不需要 JWT**，只需 username + old/new password，成功后强制登出清 token）
- `/account/findByID` — 按 ID 查用户
- `/account/findByUsername` — 按用户名查用户
- `/account/logout` — 登出（需 JWT）
- `/account/rename` — 改名（需 JWT，生成新 JWT 并使旧 token 失效）

**Video 模块** (`/video`)
- `/video/listByAuthorID` — 作者视频列表
- `/video/getDetail` — 视频详情（三级缓存 + 分布式锁防击穿）
- `/video/uploadVideo` — 上传视频文件（需 JWT，**multipart/form-data**，字段名 `file`，仅 .mp4，上限 200MB，存储到 `.run/uploads/videos/{uid}/{date}/`）
- `/video/uploadCover` — 上传封面文件（需 JWT，**multipart/form-data**，字段名 `file`，.jpg/.jpeg/.png/.webp，上限 10MB，存储到 `.run/uploads/covers/{uid}/{date}/`）
- `/video/publish` — 发布视频（需 JWT，事务写入 Video + OutboxMsg）
- `/video/delete` — 删除视频（需 JWT，仅作者，清除缓存）
- `/video/updateLikesCount` — 更新点赞数（需 JWT，内部同步用）

**Like 模块** (`/like`)
- `/like/like` — 点赞（需 JWT，滑动窗口限流 30/min/用户，MQ 优先 + 同步降级）
- `/like/unlike` — 取消点赞（需 JWT，滑动窗口限流 30/min/用户，MQ 优先 + 同步降级）
- `/like/isLiked` — 查询是否已点赞（需 JWT）
- `/like/listMyLikedVideos` — 我的点赞列表（需 JWT）

**Comment 模块** (`/comment`)
- `/comment/listAll` — 评论列表（公开）
- `/comment/publish` — 发布评论（需 JWT，滑动窗口限流 10/min/用户，MQ 优先 + 同步降级）
- `/comment/delete` — 删除评论（需 JWT，滑动窗口限流 10/min/用户，仅作者可删）

**Social 模块** (`/social`)
- `/social/follow` — 关注（需 JWT，滑动窗口限流 20/min/用户，MQ 通知下游 + API 同步写库）
- `/social/unfollow` — 取关（需 JWT，滑动窗口限流 20/min/用户）
- `/social/getAllFollowers` — 粉丝列表（需 JWT，不传 vlogger_id 则查自己）
- `/social/getAllVloggers` — 关注列表（需 JWT，不传 follower_id 则查自己）

**Feed 模块** (`/feed`)
- `/feed/listLatest` — 最新视频流（软鉴权，冷热分离，singleflight 防击穿）
- `/feed/listLikesCount` — 按点赞数排序（软鉴权，双字段游标分页）
- `/feed/listByPopularity` — 热度排行（软鉴权，Redis ZSET 快照分页，降级 MySQL）
- `/feed/listByFollowing` — 关注的人的视频（需 JWT，推拉结合 + k-way merge）

### 前端路由

| 路径 | 组件 | 说明 |
|------|------|------|
| `/` | `HomeView.vue` | 首页 Feed 流 |
| `/hot` | `HotView.vue` | 热榜 |
| `/video` | `VideoView.vue` | 视频列表/发布 |
| `/video/:id` | `VideoDetailView.vue` | 视频详情 |
| `/account` | `AccountView.vue` | 登录 |
| `/account/register` | `RegisterView.vue` | 注册 |
| `/account/change-password` | `ChangePasswordView.vue` | 改密 |
| `/settings` | `SettingsView.vue` | 设置 |
| `/u/:id` | `UserProfileView.vue` | 用户主页 |

## 5. 数据模型与状态管理

### 数据库表（MySQL，GORM AutoMigrate 自动迁移）

**Account**: id(PK), username(unique), password(bcrypt), token, follower_count

**Video**: id(PK), author_id(index), username, title, description, play_url, cover_url, create_time(auto), likes_count(GREATEST 保底), popularity(GREATEST 保底)

**Like**: id(PK), video_id(联合唯一索引), account_id(联合唯一索引), created_at

**Comment**: id(PK), username(index), video_id(index), author_id(index), content(text), created_at

**Social**: id(PK), follower_id(联合唯一索引), vlogger_id(联合唯一索引)

**OutboxMsg**: id(PK), video_id(index), author_id(index), event_type, create_time, status(pending/done)

### Redis 数据结构

| 用途 | Key 模式 | 类型 | TTL | 说明 |
|------|----------|------|-----|------|
| 鉴权 Token | `account:<id>` | STRING | 24h | 自愈：未命中回退 MySQL 后回填 |
| 视频详情缓存 | `video:detail:id=<id>` | STRING | 5min（VideoService）/ 1h（FeedService 回写） | 变更时主动 DEL |
| 全局时间线 | `feed:global_timeline` | ZSET | — | score=create_time_ms，保留最新 1000 条 |
| 热榜分钟窗 | `hot:video:1m:<yyyyMMddHHmm>` | ZSET | 2h | score=热度增量，每分钟分桶 |
| 热榜快照 | `hot:video:merge:1m:<as_of>` | ZSET | 2min | ZUNIONSTORE 按时间衰减加权合并 60 窗口（decay=0.95） |
| 用户发件箱 | `user_videos:<authorID>` | ZSET | 24h | score=create_time_ms，保留最新 50 条 |
| 用户收件箱 | `inbox:<followerID>` | ZSET | — | 概率裁剪（1%），上限 500 条 |
| 大V关注集合 | `following:bigv:<followerID>` | SET | 24h | 用户关注的大V ID |
| 粉丝数缓存 | `user:follower_count:<id>` | STRING | 0（永不过期，⚠️ 见技术债） | 判断大V用，follow/unfollow 时由 SocialWorker 写入 |
| 用户活跃时间 | `user:active:<id>` | STRING | 72h | 登录时写入（`account/service.go` Login 方法），推拉结合过滤僵尸粉用 |
| 分布式锁 | `lock:<cacheKey>` | STRING | 2s | SETNX + 随机 16 字节 token + Lua 解锁脚本（GET + compare + DEL 原子操作，防止误删他人锁） |
| 评论分页缓存 | `comment:list:video:{id}:page:{p}:size:{s}` | STRING | 30s | JSON 序列化 []Comment，写操作时只 DEL 第 1 页（⚠️ 其他页靠 TTL 自然过期） |

**评论分页缓存设计决策**（详见 `docs/optimization-report.md`）：

- 曾考虑"整体缓存"（缓存整个评论列表），但分析后否决：500 条评论 JSON 约 50KB，序列化开销大；缓存失效时全量回查 MySQL 反而更差；前端只需 20 条但返回全部数据
- 最终采用"分页缓存"：Key 带 page+size 参数，Value 从 50KB 降到 2KB（25 倍缩小），各页独立失效，并发穿透风险低
- 失效策略：评论发布/删除时只 DEL 第 1 页（最常被查询），生产环境可用 Redis SCAN + DEL 模式匹配失效所有页
| 限流计数器 | `ratelimit:<biz>:<key>` | STRING | 固定窗口 | INCR + PEXPIRE（登录/注册） |
| 滑动窗口限流 | `ratelimit:sw:<biz>:<key>` | ZSET | 滑动窗口 | ZREMRANGEBYSCORE + ZCARD + ZADD Lua（点赞/评论/关注） |

### MQ 消息体结构

所有消息共用字段：`event_id`(string), `occurred_at`(RFC3339 时间字符串)

| 事件 | Routing Key | 额外字段 |
|------|-------------|----------|
| LikeEvent | `like.like` / `like.unlike` | `action`, `user_id`, `video_id` |
| CommentEvent | `comment.publish` / `comment.delete` | `action`, `comment_id?`, `username?`, `video_id?`, `author_id?`, `content?` |
| SocialEvent | `social.follow` / `social.unfollow` | `action`, `follower_id`, `vlogger_id` |
| PopularityEvent | `video.popularity.update` | `video_id`, `change`(int64 增量) |
| TimelineEvent | `video.timeline.publish` | `video_id`, `create_time`(int64 毫秒) |
| FanoutEvent | `video.timeline.fanout` | `video_id`, `author_id`, `create_time`(int64 毫秒) |

### 环境变量

| 变量 | 必需 | 说明 | 默认值 |
|------|------|------|--------|
| `CONFIG_PATH` | 否 | YAML 配置文件路径 | `configs/config.yaml` |
| `JWT_SECRET` | 生产必需 | JWT 签名密钥 | `"feedsystem_secret"`（不安全） |

配置管理主要通过 YAML 文件（`configs/config.yaml`），包含 Server/Database/Redis/RabbitMQ/Observability 五个配置段。配置文件不存在时自动使用 `DefaultLocalConfig()` 兜底。

**配置优先级**: `-config` flag > `CONFIG_PATH` 环境变量 > 默认路径 `configs/config.yaml`。flag 解析使用 Go `flag` 包，覆盖环境变量。

**YAML 完整字段映射**（`internal/config/loadconfig.go` 的 `Config` 结构体）：

```yaml
server:
  port: 8080              # HTTP 服务端口

database:
  host: localhost          # MySQL 主机
  port: 3306               # MySQL 端口（config.yaml 用 3307，compose-local 用 3307）
  user: root               # MySQL 用户名
  password: "123456"       # MySQL 密码
  dbname: feedsystem       # 数据库名

redis:
  host: localhost          # Redis 主机
  port: 6379               # Redis 端口
  password: "123456"       # Redis 密码
  db: 0                    # Redis DB 编号

rabbitmq:
  host: localhost          # RabbitMQ 主机
  port: 5672               # RabbitMQ AMQP 端口
  username: admin          # RabbitMQ 用户名
  password: password123    # RabbitMQ 密码

observability:
  pprof:
    enabled: true          # 是否启用 pprof
    api_addr: localhost:6062   # API 进程 pprof 地址
    worker_addr: localhost:6061 # Worker 进程 pprof 地址
```

**DefaultLocalConfig 默认值**（本地开发兜底）：MySQL `localhost:3306`（root/123456/feedsystem）、Redis `localhost:6379`（密码 123456）、RabbitMQ `localhost:5672`（admin/password123）、Pprof API `localhost:6060` Worker `localhost:6061`。

> ⚠️ `DefaultLocalConfig` 的 pprof api_addr 为 `localhost:6060`，而 `configs/config.yaml` 中为 `localhost:6062`。以实际加载的配置文件为准。

### HTTP 服务器参数

| 参数 | 值 | 说明 |
|------|-----|------|
| `ReadTimeout` | 5s | 读取完整请求的超时 |
| `WriteTimeout` | 10s | 写入响应的超时（比读超时长，因涉及 DB/Redis 查询） |
| `GracefulShutdown` | 5s | 收到 SIGINT/SIGTERM 后等待在途请求完成的超时 |
| `SetTrustedProxies` | `nil` | 不信任任何代理，取 TCP 连接原始 IP（安全默认值） |

### MySQL 连接池参数

| 参数 | 值 | 说明 |
|------|-----|------|
| `MaxOpenConns` | 100 | 最大连接数（MySQL 默认 max_connections=151，留 51 给系统） |
| `MaxIdleConns` | 25 | 空闲连接数（最大连接的 1/4，平衡内存占用和连接复用） |
| `ConnMaxLifetime` | 5min | 连接最大存活时间（避免使用被 MySQL 端超时断开的过期连接） |
| `ConnMaxIdleTime` | 3min | 空闲连接最大存活时间（释放资源） |

> 优化前使用 Go `database/sql` 默认值（MaxOpenConns=0 无限制、MaxIdleConns=2），高并发下连接等待严重，P99 从 136ms 降到 <50ms。详见 `docs/optimization-report.md`。

### 前端状态管理（Pinia）

| Store | 职责 |
|-------|------|
| `auth` | JWT Token 管理（localStorage 持久化）、登录状态、用户信息解析 |
| `social` | 关注/取关状态管理 |
| `toast` | 全局消息提示 |

## 6. 代码规范与风格

### Go 后端

- **分层命名**: `entity.go`（模型）、`repo.go`（数据访问）、`service.go`（业务逻辑）、`handler.go`（HTTP 处理）
- **依赖注入**: 通过构造函数 `NewXxxService(repo, cache, mq)` 逐层注入，不使用全局变量
- **错误处理**: 底层返回 `error`，Handler 层统一返回 JSON `{"error": "message"}`，HTTP 状态码语义化。错误包装使用 `fmt.Errorf("...: %w", err)` 保持错误链
- **MQ 降级模式**: 发布失败时对失败目标降级直写，代码模式为 `if mq != nil { publish } else { fallback }`
- **缓存策略**: "先查 L1 本地 → L2 Redis → L3 MySQL，回源后异步回写"，变更时主动 `DEL` 失效
- **配置管理**: YAML 配置文件 + 环境变量 `CONFIG_PATH` 覆盖，支持默认值兜底
- **上下文传播**: 所有数据库/Redis 操作必须传入 `context.Context`
- **命名风格**: Go 标准命名（驼峰），JSON tag 使用 snake_case
- **并发安全**: 使用 `singleflight.Group` 防止缓存击穿，`sync.Mutex` 保护共享 map
- **幂等设计**: Worker 处理消息时在事务内使用 `LikeIgnoreDuplicateInTx`、`ON DUPLICATE KEY` 等保证幂等，事务保证 INSERT + UPDATE 原子执行
- **日志规范**: 使用 `log.Printf` 输出警告和错误，禁止在 Service 层使用 `log.Fatal` / `os.Exit`

### 前端

- **Vue 3 Composition API**: 使用 `<script setup>` 语法
- **TypeScript 严格模式**
- **Pinia**: 使用 Composition API 风格的 `defineStore`
- **API 调用**: 集中在 `src/api/` 目录，通过 `client.ts` 统一管理请求
- **Token 传递**: 前端通过 `Authorization: Bearer <token>` Header 传递 JWT，由 `client.ts` 的 `postJson` / `postForm` 自动注入
- **新增 API 调用规范**: 在 `src/api/` 下按模块创建文件，使用 `client.ts` 导出的两个方法：
  - `postJson<T>(path, body, options?)` — JSON 请求（绝大多数接口），自动注入 JWT，401 时自动清 token
  - `postForm<T>(path, formData, options?)` — 文件上传（`/video/uploadVideo`、`/video/uploadCover`），传 `FormData` 对象
  - `options.authRequired = true` 时，无 token 会直接抛 `ApiError(401)` 而不发请求
  - 返回值已做 JSON 解析，错误时抛 `ApiError`（含 `status` 和 `payload`）
- **无 lint/test 脚本**: `package.json` 中仅有 `dev`/`build`/`preview`，无 ESLint/Vitest
- **rolldown-vite**: `package.json` 中 `vite` 被 override 为 `npm:rolldown-vite@7.2.5`（Rust 版 Vite，构建更快），非标准 esbuild 版
- **TypeScript 版本**: `5.9.3`，使用 `@vue/tsconfig` 配置
- **App.vue**: 极简设计，仅 `<RouterView />`，无全局布局/导航栏，各 View 自行处理布局
- **前端类型定义**（`src/api/types.ts`）：
  - `FeedVideoItem.create_time` 是 `number`（毫秒时间戳），`Video.create_time` 是 `string`（RFC3339）—— 不同 API 返回不同格式
  - `FeedVideoItem` 有嵌套 `author: FeedAuthor` 对象，`Video` 是扁平的 `author_id` + `username`
  - 多种分页策略的类型：时间游标（`next_time`）、双字段游标（`next_likes_count_before` + `next_id_before`）、三字段游标（`next_latest_popularity` + `next_latest_before` + `next_latest_id_before`）
- **JWT 解码**（`src/utils/jwt.ts`）：仅解码 payload（不验证签名），正确处理 base64url 编码和 UTF-8（`TextDecoder`），失败时返回 `null` 而非抛异常
- **Auth Store**（`src/stores/auth.ts`）：localStorage key 为 `jwt_token`，支持 `syncFromStorage()` 方法实现多标签页 token 同步
- **VITE_API_BASE**: 环境变量 `VITE_API_BASE` 可覆盖 API 基础路径（默认 `/api`），配合 `vite.config.ts` 的 proxy 规则
- **路由**: 使用 `createWebHistory()`（HTML5 history 模式，非 hash 模式），`/feed` 重定向到 `/`，无路由级守卫（鉴权在组件内处理），无懒加载（所有 View 同步导入）

## 7. 常用命令

### 后端开发

```bash
# 启动 API 服务（本地开发，MySQL 3306）
cd backend_repeat && go run ./cmd

# 启动 API 服务（Compose 依赖，MySQL 3307）
cd backend_repeat && CONFIG_PATH=configs/config.compose-local.yaml go run ./cmd

# 启动 Worker 进程
cd backend_repeat && go run ./cmd/worker

# 启动 Worker（Compose 依赖）
cd backend_repeat && CONFIG_PATH=configs/config.compose-local.yaml go run ./cmd/worker

# 编译二进制
cd backend_repeat && go build -o api ./cmd
cd backend_repeat && go build -o worker ./cmd/worker

# 运行测试（当前仅 middleware/redis 和 observability 有测试）
cd backend_repeat && go test ./...

# 代码格式化
cd backend_repeat && go fmt ./...
```

### 前端开发

```bash
# 安装依赖
cd frontend && npm install

# 启动开发服务器（默认 :5173，代理 /api → 127.0.0.1:8080）
cd frontend && npm run dev

# 构建生产版本
cd frontend && npm run build

# 预览构建结果
cd frontend && npm run preview
```

### Docker Compose

```bash
# 一键启动所有服务（含 Prometheus + Grafana）
docker compose up -d --build

# 仅启动依赖服务
docker compose up -d mysql redis rabbitmq

# 查看日志
docker compose logs -f backend worker

# 停止所有服务
docker compose down
```

### 监控服务访问

| 服务 | 地址 | 说明 |
|------|------|------|
| Grafana | http://localhost:3001 | 账号 `admin`，密码 `admin123`，Dashboard 名 "FeedSystem Monitoring" |
| Prometheus | http://localhost:9090 | 查看 Targets 确认 `feedsystem-api` 状态为 UP |
| /metrics 端点 | http://localhost:8080/metrics | Prometheus 抓取端点，浏览器可直接访问查看原始指标 |
| RabbitMQ 管理台 | http://localhost:15672 | 账号 `admin`，密码 `password123`，查看队列深度/消费速率/连接数 |

### 一键启动脚本

```bash
# 启动全部（后端 + Worker + 前端 + Redis + RabbitMQ）
./start.sh

# 仅启动后端（不启动前端）
START_FRONTEND=0 ./start.sh

# 仅启动前端
START_BACKEND=0 START_WORKER=0 ./start.sh

# 不自动启动 Redis（使用已有的 Redis 实例）
START_REDIS=0 ./start.sh
```

## 8. 开发工作流与注意事项

### MQ 拓扑

| Exchange | Queue | Binding Key | Consumer | 说明 |
|----------|-------|-------------|----------|------|
| `social.events` | `social.events` | `social.*` | SocialWorker | 落库 + bigv SET + inbox 回填 |
| `like.events` | `like.events` | `like.*` | LikeWorker | 幂等落库 + 更新 likes_count |
| `comment.events` | `comment.events` | `comment.*` | CommentWorker | 幂等落库 + 更新 popularity |
| `video.popularity.events` | `video.popularity.events` | `video.popularity.*` | PopularityWorker | 更新热榜分钟窗 |
| `video.timeline.events` | `video.timeline.update.queue` | `video.timeline.publish` | Consumer (API 进程内) | 写 feed:global_timeline |
| `video.timeline.events` | `video.timeline.fanout.queue` | `video.timeline.fanout` | FanoutWorker | 推拉结合推路径 |

### 关键设计决策

1. **API 与 Worker 分离**: API 进程快速响应，Worker 异步处理重逻辑，可独立扩缩容
2. **Outbox 模式**: 视频发布使用本地消息表 (`OutboxMsg`) + 轮询器（间隔 1s，批量拉取 100 条 pending），保证 DB 写入与 MQ 投递的最终一致性。OutboxPoller 双投递（TimelineMQ + FanoutMQ）的失败语义：TimelineMQ 失败 → 跳过本轮；TimelineMQ 成功 + FanoutMQ 失败 → 跳过（下轮重投两条，Timeline 消费者通过 ZADD 幂等，Fanout 消费者通过 ZADD 幂等）
3. **推拉结合 Feed**:
   - 普通用户发视频 → FanoutWorker fanout 到**活跃**粉丝 `inbox:{followerID}` ZSET（推路径）
   - 大V（粉丝 >= 10000）发视频 → 只写 `user_videos:{authorID}` ZSET（拉路径，读时合并）
   - **粉丝活跃度过滤**：只推给 3 天内登录过的粉丝（`user:active:{userID}` TTL 72h），僵尸粉不推，走拉路径兜底
   - SocialWorker 维护 `following:bigv:{followerID}` SET，记录用户关注了哪些大V
   - FeedService.ListByFollowing 读路径：并行读 inbox + 大V列表 → Pipeline 探测所有大V活跃度 → 按 newestAt 降序分配拉取配额 → 真正的 k-way merge（堆大小 = K，O(N log K)）
   - 公平提前终止：Pipeline 批量查所有大V最新时间戳取 max，只有所有大V都比 inbox 最老才跳过拉路径
   - **拉取配额跳过冷大V**：分配配额时跳过 `newestAt <= inboxOldest` 的大V，避免无意义的 Redis 查询
   - 活跃度优先拉取：按大V最新内容时间戳降序排序，从最活跃的大V开始分配配额，每个大V最多 10 条
4. **热榜滑动窗口**: 按分钟分桶写入 ZSET，查询时 `ZUNIONSTORE` 按时间衰减加权聚合最近 60 个窗口（decay=0.95，越老权重越小），生成快照
5. **三级缓存防击穿**: L1 本地缓存（go-cache）→ L2 Redis → L3 MySQL，singleflight（4 处）+ 分布式锁（GetDetail），单机下效果一样
6. **冷热分离**: ListLatest 热数据走 Redis ZSET，冷尾走 MySQL，ZSET 为空时 singleflight 防并发重建

### 推拉结合设计参考资料

- [Stream-Framework](https://github.com/tschellenbach/Stream-Framework)（4,750 stars）— 最成熟的开源 Feed 系统，概率裁剪 `trim_chance=0.01`、关注回填 `follow_activity_limit=5000` 等设计均参考此项目
- Twitter "Timelines at Scale"（Raffi Krikorian / Nick Kallen, QCon 2012）— 推拉结合经典案例
- 微博 Feed 流架构设计 — 大V阈值参考（5,000-10,000 粉丝）
- 大V阈值选择依据：Twitter ~500,000（用户基数大）、微博 ~5,000-10,000（中文社区标准）、Stream-Framework 无硬编码（由业务层决定）
- 详细设计文档：`backend_repeat/docs/push-pull-hybrid-feed.md`（含 Mermaid 流程图、文件修改清单、各场景开销分析）

### Worker Ack/Nack 策略

- 消费成功：`msg.Ack(false)`
- 业务逻辑报错（可重试）：`msg.Nack(false, true)` 重新入队
- 消息格式错误（不可重试）：`msg.Nack(false, false)` 丢弃

### Worker 背压控制

`cmd/worker/main.go` 中设置 `Qos(prefetchCount=50, prefetchSize=0, prefetchGlobal=false)`：
- **prefetchCount=50**：RabbitMQ 最多向单个 Worker 推送 50 条未确认消息，消费完才推新的。防止 Worker 积压过多消息导致内存溢出
- **prefetchGlobal=false**：每个 Channel 独立计数（非整个 Connection 共享）
- 设太大 → 内存占用高、消息堆积在 Worker 端；设太小 → Worker 频繁等待 Broker 推送，吞吐下降

### TOCTOU 竞态处理

**TOCTOU（Time-of-Check to Time-of-Use）**：在"检查"和"使用"之间的时间窗口内，状态可能被其他线程/进程改变。

| 操作 | 策略 | 原因 |
|------|------|------|
| 点赞 | **不预检查**，直接 INSERT，依赖唯一索引 (video_id, account_id) 兜底 catch 1062 | 高频操作，预检查的额外 DB 查询成本不值得 |
| 关注 | **保留预检查**（IsFollowed），给用户"已经关注了"的友好提示，uniqueIndex 兜底 | 低频操作，用户体验优先 |

### bcrypt 性能特征

bcrypt 哈希/验证是 CPU 密集型操作，默认 cost=10 需要 ~100ms。这是故意设计的（防暴力破解），不是 bug。高并发登录场景下 100ms/请求意味着单核只能支撑 ~10 QPS 的登录。仅影响 `/account/login` 和 `/account/register`，其他接口不受影响。

### Redis Cluster 兼容性

项目中 3 个 Lua 脚本（限流、解锁、滑动窗口）均只操作单 key，天然兼容 Redis Cluster（无 CROSSSLOT 问题）。但 `ZUNIONSTORE` 合并 60 个热度窗口是多 key 操作，迁移到 Cluster 需用 Hash Tag 保证相关 key 落在同一 slot：`hot:video:1m:{video}:202605061435`。

### 熔断器配置参数

`internal/middleware/redis/breaker.go` 使用 `sony/gobreaker v2`，配置如下：

| 参数 | 值 | 说明 |
|------|-----|------|
| `MaxRequests` | 1 | HalfOpen 状态下只允许 1 个探测请求 |
| `Interval` | 60s | Closed 状态滚动窗口，60s 后失败计数重置 |
| `Timeout` | 10s | Open 状态持续 10s 后转为 HalfOpen |
| `ConsecutiveFailures` | 5 | 连续 5 次失败触发 Open |

**redis.Nil 特殊处理**：`Execute` 方法内部判断 `IsMiss(err)`（即 `redis.Nil`），将其报告为成功（不计入熔断失败），但仍然将原始 `redis.Nil` 返回给调用方。这保证缓存穿透不会误触熔断器。

**IsBreakerOpen 行为**：对 `open` 和 `half_open` 状态均返回 `true`，用于 Pipeline 路径（无法使用 `Execute` 封装）。注意：这意味着 HalfOpen 探测期 Pipeline 也会跳过 Redis，可能过度降级。

**ErrBreakerOpen 哨兵错误**：`breaker.go` 定义 `ErrBreakerOpen = errors.New("circuit breaker is open")`。各 Service 检测此错误触发 MySQL 降级路径：
- `FeedService.ListLatest` → `listLatestFromDB`（纯 MySQL 查询）
- `FeedService.ListByFollowing` → `listByFollowingFromDB`（MySQL 子查询）
- Worker 的 `handleDelivery` → sleep 1s 后 Nack+requeue（避免热循环）

### ListByFollowing 超时与降级

### ListByFollowing 超时与降级

`FeedService.ListByFollowing` 整个 Redis 路径有 **200ms 硬超时**（`context.WithTimeout`）。超时后自动降级到 `listByFollowingFromDB`（MySQL 查询），保证用户体验不受 Redis 慢查询影响。

### 推拉结合关键常量

| 常量 | 值 | 文件 | 说明 |
|------|-----|------|------|
| `BigVThreshold` | 10000 | `fanoutworker.go` | 大V 粉丝阈值，编译时常量，生产环境建议改为配置中心动态调整 |
| `inboxCap` | 500 | `fanoutworker.go` | 收件箱 ZSET 上限 |
| `userVideosCap` | 50 | `fanoutworker.go` | 发件箱 ZSET 上限 |
| `fanoutBatchSize` | 100 | `fanoutworker.go` | Pipeline 批量写入 chunk 大小 |
| `trimChance` | 0.01 | `fanoutworker.go` | 每次写入时 1% 概率裁剪收件箱（参考 Stream-Framework） |
| 收件箱回填数 | 50 | `socialworker.go` | 关注非大V时回填最近 50 条视频 |
| 大V拉取配额 | 10/大V | `feed/service.go` | 每个大V最多拉取 10 条 |

**概率裁剪设计**（参考 Stream-Framework `trim_chance=0.01`）：每次 FanoutWorker 写入收件箱时，以 1% 概率触发 `ZREMRANGEBYRANK` 裁剪到 `inboxCap`。相比每次写入都裁剪，概率裁剪将裁剪开销降低 100 倍，收件箱可能短暂超过 500 条但在下次裁剪时恢复。这是流式系统中经典的"摊还"优化。

### MQ 写入模式差异

不同模块采用不同的 MQ 写入模式，这是有意为之的设计决策：

| 模块 | 模式 | 原因 |
|------|------|------|
| **Like / Comment** | MQ 优先 + 同步降级 | 高频写操作，MQ 成功即返回（低延迟），失败时降级到 MySQL 事务保证一致性 |
| **Social** | DB 优先 + MQ 通知（fire-and-forget） | 关注关系必须强一致（立即可见），MQ 仅通知下游（fanout/bigv SET），**MQ 失败静默忽略不降级**（与 Like/Comment 的降级模式不同） |
| **Video Publish** | Outbox 模式 | 事务写入 Video + OutboxMsg，OutboxPoller 轮询投递 TimelineMQ + FanoutMQ，保证最终一致性 |

### UpdatePopularityCache 的双重 Context 策略

`internal/video/popularity_cache.go:UpdatePopularityCache` 在一次调用中使用了两个不同的 context：

1. **`context.Background()`** 用于 `cache.Del()`（删除视频详情缓存）— 即使用户请求已取消（客户端断开），缓存失效仍应完成，否则下次读到脏数据
2. **`context.WithTimeout(context.Background(), 50ms)`** 用于 `cache.ZincrBy()` + `cache.Expire()`（更新热度窗口）— 独立 50ms 超时，防止单次 Redis 操作卡住 goroutine

设计原则：**缓存失效是"必须完成"的操作（用 Background），缓存写入是"尽力而为"的操作（用带超时的 Context）**。

### 进程归属

| 组件 | 运行进程 | 说明 |
|------|----------|------|
| OutboxPoller | API 进程 | `router.go` 中 `SetRouter` 内启动，轮询 `outbox` 表 |
| Timeline Consumer | API 进程 | `router.go` 中 `SetRouter` 内启动，消费 `video.timeline.update.queue`，写入 Redis `feed:global_timeline` |
| SocialWorker / LikeWorker / CommentWorker / PopularityWorker / FanoutWorker | Worker 进程 | `cmd/worker/main.go` 中启动 |

> **Worker 不执行 AutoMigrate**：只有 API 进程（`cmd/main.go`）调用 `db.AutoMigrate`，Worker 进程跳过。原因：(1) 避免多进程并发 DDL 竞争；(2) Worker 启动时表结构已由 API 进程保证就绪；(3) 职责分离——API 负责 Schema，Worker 只消费。

### Comment fallback 事务内二次校验

`CommentService.Publish` 的 MQ fallback 路径中，事务内会**重新检查视频存在性**（`videoRepo.IsExist`），而不仅仅依赖事务前的检查。这防止了以下竞态：事务前检查视频存在 → 另一个请求删除视频 → 事务内插入评论（引用已删除的视频）。

### SocialWorker inbox 回填

当用户关注一个**非大V**时，`SocialWorker.onFollow` 调用 `backfillInbox`：从 `user_videos:{vloggerID}` ZSET 取最近 50 条视频，写入 `inbox:{followerID}` ZSET。这解决了"关注后看不到历史内容"的冷启动问题。大V 不走回填（大V内容走拉路径）。

### 其他超时与降级常量

| 场景 | 超时 | 文件 | 说明 |
|------|------|------|------|
| ListByFollowing Redis 路径 | 200ms | `feed/service.go` | 超时降级 `listByFollowingFromDB` |
| ListByPopularity 热榜合并 | 80ms | `feed/service.go` | ZUNIONSTORE 60 窗口 |
| ListLatest ZSET 重建 | 2000ms | `feed/service.go` | singleflight 从 MySQL 回填 1000 条 |
| FeedService L2 Redis MGet | 50ms | `feed/service.go` | 批量查视频详情 |
| VideoService 分布式锁获取 | 50ms | `video_service.go` | 超时后降级到 MySQL |
| VideoService 锁轮询 | 5 次 × 20ms | `video_service.go` | 未获锁时 spin-wait |
| SocialWorker onFollow | 500ms | `socialworker.go` | 关注后 Redis 维护 |
| SocialWorker onUnfollow | 200ms | `socialworker.go` | 取关后 Redis 维护 |
| 所有 Redis 操作 | 50ms | 各 service | 独立 `context.WithTimeout(context.Background(), 50ms)` |

### 限流数值汇总

| 接口 | 限流方式 | 窗口 | 限值 | Key 维度 |
|------|----------|------|------|----------|
| `/account/login` | 固定窗口（INCR+PEXPIRE） | 1min | 10 次 | IP |
| `/account/register` | 固定窗口（INCR+PEXPIRE） | 1min | 10 次 | IP |
| `/like/like` + `/like/unlike` | 滑动窗口（ZSET+Lua） | 1min | 30 次 | accountID |
| `/comment/publish` + `/comment/delete` | 滑动窗口（ZSET+Lua） | 1min | 10 次 | accountID |
| `/social/follow` + `/social/unfollow` | 滑动窗口（ZSET+Lua） | 1min | 20 次 | accountID |

> 限流中间件在 Redis 不可用或熔断器 Open 时 fail-open（放行），不影响核心功能。

**Lua 脚本细节**：
- **固定窗口**（`incrementWithExpireScript`）：原子执行 `INCR` + `PEXPIRE`，但仅在 count==1 时设置过期（首次请求设窗口，后续请求只增计数），避免每次请求都刷新 TTL 导致窗口无限延长
- **滑动窗口**（`slidingWindowScript`）：原子执行 `ZREMRANGEBYSCORE` + `ZCARD` + `ZADD` + `PEXPIRE`，4 条命令 1 次网络 RTT（~0.3ms 额外开销 vs 固定窗口）。member 使用 `纳秒时间戳:原子计数器` 组合保证唯一性（毫秒级可能碰撞，纳秒级在高并发下也可能碰撞，加计数器彻底消除）
- **时钟漂移规避**：滑动窗口的时间戳由 Go 侧传入（`ARGV[3]`），不使用 Redis `TIME` 命令，避免 Redis 服务器与应用服务器时钟不同步
- **熔断器绕过**：滑动窗口脚本调用前检查 `cache.IsBreakerOpen()`，若熔断器开启则直接 fail-open（Lua 脚本无法用 `breaker.Execute` 封装）

### 已知技术债与注意事项

- `start.sh` 中 `BACKEND_DIR` 默认指向 `backend/`，但实际代码在 `backend_repeat/`，需手动设置或修改
- `outboxworker.go` 中 `StartConsumer` 的 `defer cancel()` 在 for 循环内（line 80），defer 直到 goroutine 退出才执行，每次迭代创建的 context 不会被取消，导致 context 泄漏（已知 bug）
- Worker 进程中 Redis/RabbitMQ 连接失败会 `log.Fatalf` 直接退出，API 进程则降级为 nil
- 所有 MQ 生产者和消费者共享同一个 `*amqp.Channel`（amqp091-go Channel 非线程安全，高并发下有协议帧冲突风险，当前单实例低并发场景下可接受）
- 无 RabbitMQ 自动重连机制：连接断开后 publish 失败会触发降级直写，但不会自动恢复 MQ 路径
- 无 Publisher Confirms：`PublishJSON` 返回 nil 不代表 broker 已确认接收，配合降级直写可接受
- `StartOutboxPoller` 无 `ctx.Done()` 退出检查，进程停止时轮询器不会优雅退出
- 前端 `vite.config.ts` 使用 `127.0.0.1` 而非 `localhost` 避免 Windows IPv6 解析问题
- 视频文件存储在本地 `.run/uploads/` 目录，生产环境需替换为对象存储（OSS/S3/MinIO）
- JWT 密钥默认为 `"feedsystem_secret"`，生产环境必须通过 `JWT_SECRET` 环境变量覆盖
- `user:follower_count:<id>` 缓存写入时 TTL 为 0（永不过期），存在于 `fanoutworker.go` 和 `socialworker.go`。如果 follow/unfollow 事件丢失，缓存的粉丝数将永久脏数据，影响大V判断。生产环境应设置合理 TTL（如 24h）或在 follow/unfollow 时主动更新
- 评论缓存失效不完整：`invalidateCommentCache` 只删除 `page:1:size:20` 的缓存 key，其他 page/size 组合靠 30s TTL 自然过期。代码注释已承认此局限，生产环境可用 Redis SCAN + DEL 模式匹配或版本号方案
- `EMPTY_DB` 短路标记是裸字符串常量（`"EMPTY_DB"`），非类型化常量，修改时需全局搜索替换，存在拼写错误风险
- 测试覆盖率低：`middleware/redis/breaker_test.go`（熔断器）、`middleware/redis/redis_test.go`（限流 Lua）、`middleware/ratelimit/sliding_window_test.go`（滑动窗口）、`observability/pprof_test.go`、`worker/likeworker_test.go`、`feed/service_test.go`，其余业务层无测试
- `commentworker.go` 的 `applyPublish` 传 `tx=nil` 给 `ChangePopularity`，评论插入和热度更新**不在同一事务中**（与 LikeWorker 的事务内 INSERT + UPDATE likes_count + UPDATE popularity 原子执行不一致）。后果：如果 `ChangePopularity` 失败，评论已插入但热度未更新，且 MQ 消息已 Ack 不会重试
- `commentworker.go` 的 `applyDelete` 不扣减 popularity，与 publish 路径不对称（删除评论不影响热度分数）
- `/account/login` 对密码错误返回 500 而非 401（应语义化为 401 Unauthorized）
- `/video/listByAuthorID` 的 SQL 查询无 LIMIT 子句，作者视频量大时可能返回过多数据
- Redis nil-safe 模式不一致：`cache.go` 的 `SetBytes`/`GetBytes`/`Del` 等对 nil client 静默返回零值，但 `set.go` 的 `SAdd`/`SMembers` 和 `zset.go` 的 `ZCard`/`MGet` 等返回显式 `errors.New("redis client not initialized")`
- `UpdatePopularityCache` 在 LikeService/CommentService 的 MQ fallback 路径和 `PopularityWorker` 中都被调用，理论上 MQ 消息和 fallback 不会同时执行（MQ 成功则不走 fallback），但如果 MQ 消息延迟到达而 fallback 已执行，存在短暂双写风险（实际影响小，因为操作是幂等的 ZINCRBY）

## 9. 可观测性（Prometheus + Grafana）

### 架构

```
Gin 请求 → MetricsMiddleware（自动采集）→ Handler
                  ↓
         Prometheus 指标注册表
                  ↓
         Prometheus (:9090) ← 每 15s scrape /metrics
                  ↓
         Grafana (:3001) → Dashboard 展示
```

### 指标清单

| 指标名 | 类型 | 标签 | 采集方式 | 说明 |
|--------|------|------|----------|------|
| `feedsystem_http_requests_total` | Counter | method, path, status_code | Gin middleware 自动 | HTTP 请求总量 |
| `feedsystem_http_request_duration_seconds` | Histogram | method, path | Gin middleware 自动 | HTTP 请求延迟 |
| `feedsystem_redis_operations_total` | Counter | operation, status | cache.go + zset.go + set.go 手动埋点 | Redis 操作计数（status: success/error/miss） |
| `feedsystem_redis_operation_duration_seconds` | Histogram | operation, status | cache.go + zset.go + set.go 手动埋点 | Redis 操作延迟 |
| `feedsystem_mq_messages_published_total` | Counter | exchange, routing_key | PublishJSON 手动埋点 | MQ 消息发布数 |
| `feedsystem_mq_messages_consumed_total` | Counter | queue | 所有 Worker handleDelivery 手动（IncrConsumed） | MQ 消息消费数 |
| `feedsystem_circuit_breaker_state_changes_total` | Counter | to_state | breaker.go OnStateChange | 熔断器状态切换（closed/open/half_open） |
| `feedsystem_circuit_breaker_rejections_total` | Counter | — | breaker.go Execute | 熔断器拒绝请求数 |
| `feedsystem_ratelimit_rejections_total` | Counter | limiter, prefix | ratelimit 中间件 | 限流拒绝数 |

### 采集细节

- **MetricsMiddleware 排除 `/metrics`**：避免 Prometheus scrape 请求污染业务指标
- **`FullPath()` vs `URL.Path`**：使用 `c.FullPath()`（路由模板如 `/video/getDetail`）而非 `c.Request.URL.Path`（含参数如 `/video/123`），防止标签基数爆炸
- **Histogram buckets**：HTTP 指标从 1ms 起（`[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5]`），Redis 指标从 0.1ms 起（`[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1]`），Redis 更精细因为预期操作更快
- **pprof 服务**：独立端口运行（API `:6060`，Worker `:6061`），`ReadTimeout: 5s`，无 `WriteTimeout`（长 profile 请求需要），优雅关闭超时 3s
- **缓存命中率估算**：通过 `Redis GET QPS / HTTP QPS` 比值估算（约 80%），无专用 cache_hit/cache_miss 计数器（已知指标缺口）

### 新增指标流程

1. 在 `internal/observability/metrics.go` 中定义指标变量
2. 在 `init()` 中 `prometheus.MustRegister()`
3. 在业务代码中调用 `.Inc()` / `.Observe()` / `.Add()` 记录数据
4. 重启服务后 `/metrics` 端点自动输出新指标

### 关键文件

| 文件 | 职责 |
|------|------|
| `internal/observability/metrics.go` | 所有 Prometheus 指标定义 + 注册 |
| `internal/observability/middleware.go` | Gin middleware，自动采集 HTTP QPS + 延迟 |
| `internal/observability/pprof.go` | pprof 性能分析（独立端口） |
| `internal/middleware/redis/cache.go` | Redis 操作埋点（get/set/del 计时） |
| `internal/middleware/redis/breaker.go` | 熔断器封装（sony/gobreaker v2），保护所有 Redis 调用 |
| `internal/middleware/rabbitmq/rabbitmq.go` | MQ 发布计数 + `IncrConsumed()` 消费计数 |
| `configs/prometheus.yml` | Prometheus scrape 配置 |
| `configs/grafana/provisioning/` | Grafana 数据源 + Dashboard 自动导入 |

### Grafana Dashboard 面板

1. HTTP QPS by path（每接口请求量）
2. HTTP QPS by status code（按状态码分组）
3. HTTP P50/P95/P99 延迟
4. HTTP P95 延迟 by path
5. Redis 操作 QPS
6. Redis 操作 P95 延迟
7. MQ 消息发布量
8. MQ 消息消费量

### 压测与性能优化

压测工具：`hey`（Go 写的 HTTP 压测工具）

**hey 命令示例**：

```bash
# 固定 RPS 模式（更接近真实生产流量模式）
hey -z 60s -q 100 -c 10 -m POST -H "Content-Type: application/json" \
    -d '{"video_id":1,"page":1}' \
    http://localhost:8080/comment/listAll

# 总请求数模式（找系统极限）
hey -n 50000 -c 500 -m POST -H "Content-Type: application/json" \
    -d '{"video_id":1}' \
    http://localhost:8080/video/getDetail
```

**关键 PromQL 查询**：

```promql
# QPS by path
rate(feedsystem_http_requests_total[1m])

# P95 延迟
histogram_quantile(0.95, rate(feedsystem_http_request_duration_seconds_bucket[5m]))

# 熔断器开启频率（每分钟切到 open 的次数）
rate(feedsystem_circuit_breaker_state_changes_total{to_state="open"}[1m])

# 限流拒绝率
rate(feedsystem_ratelimit_rejections_total[1m])
```

**核心数据（优化后）**：

| 场景 | 吞吐量 | P50 | P95 | P99 | 成功率 |
|------|--------|-----|-----|-----|--------|
| comment/listAll 200c | 20,220 req/s | 4.9ms | 12.7ms | 34.5ms | 100% |
| video/getDetail 200c | 24,546 req/s | 2.9ms | 6.7ms | 39.7ms | 100% |
| comment/listAll 500c | 14,422 req/s | 10.3ms | 35.1ms | 89.1ms | 100% |
| 混合 200×3 并行 | 5,076~10,392 | 1.6~2.9ms | 6.9~11.5ms | — | 100% |

**优化项**：
1. comment/listAll 加 Redis 分页缓存（TTL 30s，写操作时 DEL）→ 吞吐量 +246%，P95 -56%
2. MySQL 连接池调优（MaxOpenConns=100, MaxIdleConns=25）→ video/getDetail P95 -96%

**详细报告**：
- `docs/stress-test.md` — 完整压测数据
- `docs/optimization-report.md` — 优化方案与对比
- `docs/bottleneck-analysis.md` — 瓶颈分析
- `docs/observability-and-optimization.md` — 合并版（面试用）

## 10. AI 辅助约定

### 元规则

- 当本文档与现有代码冲突时，**以现有代码为准**，但请指出冲突点
- 修改代码前，先读取目标文件的最新内容，不要仅依赖本文档的描述
- 涉及多模块修改时，按依赖顺序读取：**Entity → Repository → Service → Handler**
- 不确定某函数/常量是否存在时，先用 Grep 搜索代码库，不要猜测

### 代码修改规范

- **分层一致性**: 修改业务逻辑时，确保 Handler → Service → Repository 的调用链完整，不跨层
- **MQ 降级**: 新增 MQ 相关功能时，必须实现降级直写逻辑（`if mq != nil { ... } else { fallback }`）
- **缓存失效**: 涉及数据变更时，必须主动 `DEL` 相关 Redis 缓存 key（video:detail、feed 缓存、comment:list 等）
- **三级缓存一致性**: 修改视频数据时，需同时失效 L1 本地缓存 + L2 Redis 缓存
- **上下文传播**: 新增数据库/Redis 操作时，必须传入 `context.Context`
- **JSON 响应格式**: 统一使用 `gin.H{"key": value}`（项目未使用统一 Response 结构体）
- **错误返回**: 使用 `http.StatusBadRequest` / `StatusInternalServerError` 等语义化状态码
- **幂等性**: Worker 处理消息时必须保证幂等（重复投递不产生副作用）
- **时间戳**: 使用消息体中的 `occurred_at` / `create_time` 字段，不要用 `time.Now()` 重新生成

### 硬性约束

- 所有 API 接口使用 **POST** 方法（RESTful 风格在此项目中不适用）
- 用户 ID **仅从 JWT 中提取**（通过 `GetAccountID(c)`），不信任客户端传参
- 密码必须使用 **bcrypt** 哈希存储
- Redis 和 RabbitMQ 均为 **可选依赖**，核心功能在两者不可用时必须降级可用
- 数据库使用 GORM **AutoMigrate** 自动迁移，不要手动写 SQL DDL
- 视频发布必须使用 **Outbox 模式**（事务写入 Video + OutboxMsg），不直接发 MQ
- 大V判断必须读取 `fanoutworker.go` 中的 `BigVThreshold` 常量（10000），严禁硬编码魔法数字

### Anti-patterns（绝对不要这样做）

**后端**：

```
// ❌ 错误：Handler 直接调用 Repository（跨层）
func (h *VideoHandler) GetDetail(c *gin.Context) {
    video, _ := h.repo.GetDetail(c.Request.Context(), id)  // 跨层！
    c.JSON(200, video)
}

// ✅ 正确：Handler → Service → Repository
func (h *VideoHandler) GetDetail(c *gin.Context) {
    video, _ := h.service.GetDetail(c.Request.Context(), id)
    c.JSON(200, video)
}
```

```
// ❌ 错误：信任客户端传来的 user_id
var req struct { UserID uint `json:"user_id"` }
c.ShouldBindJSON(&req)
service.DoSomething(req.UserID)  // 不安全！

// ✅ 正确：从 JWT 中提取
accountID, err := jwt.GetAccountID(c)
if err != nil {
    c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
    return
}
service.DoSomething(accountID)
```

```
// ❌ 错误：MQ 发布失败就丢弃
err := mq.Publish(ctx, event)
if err != nil {
    return err  // 数据不一致！
}

// ✅ 正确：MQ 降级直写
if mq != nil {
    if err := mq.Publish(ctx, event); err != nil {
        // 降级：直接写 MySQL/Redis
        repo.DirectWrite(ctx, data)
    }
} else {
    repo.DirectWrite(ctx, data)
}
```

```
// ❌ 错误：修改数据后不清理缓存
func (s *VideoService) Delete(ctx context.Context, id uint) error {
    return s.repo.DeleteVideo(ctx, id)  // 缓存未清理！
}

// ✅ 正确：删除后主动失效缓存
func (s *VideoService) Delete(ctx context.Context, id uint) error {
    if err := s.repo.DeleteVideo(ctx, id); err != nil {
        return err
    }
    s.cache.Del(ctx, fmt.Sprintf("video:detail:id=%d", id))
    s.localcache.Delete(fmt.Sprintf("video:detail:id=%d", id))
    return nil
}
```

```
// ❌ 错误：新增接口用 GET 方法
r.GET("/video/getDetail", handler)

// ✅ 正确：所有接口都用 POST
r.POST("/video/getDetail", handler)
```

```
// ❌ 错误：在 for 循环内 defer cancel()（context 提前取消）
for msg := range msgs {
    ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
    defer cancel()  // 直到函数返回才执行，循环内会泄漏！
    // ...
}

// ✅ 正确：循环内用匿名函数或显式 cancel
for msg := range msgs {
    func() {
        ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
        defer cancel()
        // ...
    }()
}
```

```
// ❌ 错误：Redis 操作不判 nil（降级场景会 panic）
func (s *VideoService) GetDetail(ctx context.Context, id uint) (*Video, error) {
    val, _ := s.cache.GetBytes(ctx, key)  // cache 可能为 nil！
    // ...
}

// ✅ 正确：先判 nil 再操作
if s.cache != nil {
    val, err := s.cache.GetBytes(ctx, key)
    // ...
}
```

```
// ❌ 错误：Service 层使用 log.Fatal / os.Exit（会导致进程退出）
func (s *VideoService) PublishVideo(ctx context.Context, video *Video) error {
    if err != nil {
        log.Fatalf("publish failed: %v", err)  // 不可恢复！
    }
}

// ✅ 正确：返回 error，让调用方决定如何处理
func (s *VideoService) PublishVideo(ctx context.Context, video *Video) error {
    if err != nil {
        return fmt.Errorf("publish failed: %w", err)
    }
}
```

**前端**：

```
// ❌ 错误：在组件内直接调用 fetch（绕过统一请求管理）
const res = await fetch('/api/video/getDetail', { method: 'POST', ... })

// ✅ 正确：通过 client.ts 的 postJson 调用
import { postJson } from '@/api/client'
const data = await postJson('/video/getDetail', { id })
```

```
// ❌ 错误：在 Store 外部管理 JWT Token
localStorage.setItem('token', token)  // 散落在各组件中

// ✅ 正确：通过 auth store 统一管理
import { useAuthStore } from '@/stores/auth'
const auth = useAuthStore()
auth.setToken(token)
```

```
// ❌ 错误：前端做 JWT 签名验证（前端是纯消费者，不做签名验证）
const payload = jwt.verify(token, secret)  // 前端没有 secret！

// ✅ 正确：仅解码 payload（不验证签名）
import { decodeJwtPayload } from '@/utils/jwt'
const payload = decodeJwtPayload(token)
```

### 测试约定

- 当前业务层无测试，AI 在新增核心 Service 逻辑时，**应主动生成 `_test.go` 单元测试文件**
- 测试文件放在同目录下（如 `video/video_service_test.go`）
- 使用 Go 标准 `testing` 包 + `testify` 断言库
- Repository 层测试可用真实 MySQL（项目无 mock 框架）
- 前端当前无测试框架（package.json 无 Vitest），暂不强制要求

### 输出格式

根据问题复杂度选择格式：

**简单问题**（如"这个接口怎么调用"）：直接回答，不要三段式。

**复杂修改**（如"重构 Worker 的幂等逻辑"）：
1. **问题分析**: 说明当前代码的问题或瓶颈
2. **修改方案**: 给出修改前后的代码对比
3. **影响评估**: 说明修改对其他模块的影响、是否需要同步修改
