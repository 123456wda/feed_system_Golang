# 🎬 FeedSystem — 高性能短视频 Feed 流系统

> Go + Gin + GORM + MySQL + Redis + RabbitMQ + Vue 3 | API + Worker 双进程架构 | Docker Compose 一键部署

## 项目简介

一款基于 Go 的高性能短视频 Feed 流系统，提供账号、视频、点赞、评论、关注与多策略 Feed 流等完整功能。通过 **Redis 三级缓存**、**RabbitMQ 异步消息队列**、**推拉结合 Feed** 等架构设计，在高并发场景下实现低延迟、高吞吐。

## 技术栈

| 层级 | 技术 |
|------|------|
| **后端** | Go 1.26 · Gin · GORM · bcrypt · JWT (HS256) |
| **存储** | MySQL 8.0 · Redis 7 · go-cache (本地缓存) |
| **消息队列** | RabbitMQ 3 (Topic Exchange) |
| **前端** | Vue 3 · TypeScript · Vite (rolldown-vite) · Pinia · Vue Router |
| **可观测性** | Prometheus · Grafana · pprof |
| **容器化** | Docker · Docker Compose (8 服务编排) |
| **高可用** | 三级缓存 · 分布式锁 · 熔断器 (sony/gobreaker) · 限流 · singleflight |

## 快速启动

### Docker Compose（推荐）

```bash
# 一键启动全部服务（MySQL + Redis + RabbitMQ + 后端 + Worker + 前端 + Prometheus + Grafana）
docker compose up -d --build

# 访问前端
open http://localhost:5173

# 访问 Grafana Dashboard（admin / admin123）
open http://localhost:3001
```

### 本地开发

```bash
# 1. 启动依赖服务
docker compose up -d mysql redis rabbitmq

# 2. 启动后端 API
cd backend_repeat && go run ./cmd

# 3. 启动 Worker（另一个终端）
cd backend_repeat && go run ./cmd/worker

# 4. 启动前端（另一个终端）
cd frontend && npm install && npm run dev
```

### 监控服务

| 服务 | 地址 | 说明 |
|------|------|------|
| 前端 | http://localhost:5173 | Vue 3 SPA |
| API | http://localhost:8080 | 后端接口 |
| Grafana | http://localhost:3001 | Dashboard（admin / admin123） |
| Prometheus | http://localhost:9090 | 指标查询 |
| RabbitMQ | http://localhost:15672 | MQ 管理台（admin / password123） |

## 系统架构

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────────────────────┐
│   Client    │────▶│  Gin Router  │────▶│  Handler → Service → Repository │
│  (Vue 3)    │     │  JWT + 限流   │     │         ↕                       │
└─────────────┘     └──────────────┘     │  L1 本地缓存 → L2 Redis → L3 MySQL│
                                          └───────────────┬─────────────────┘
                                                          │
                                          ┌───────────────▼─────────────────┐
                                          │     RabbitMQ (Topic Exchange)    │
                                          └───────────────┬─────────────────┘
                                                          │
                                          ┌───────────────▼─────────────────┐
                                          │   Worker 进程（5 个消费者）       │
                                          │   落库 / 计数 / 热榜 / Fanout    │
                                          └─────────────────────────────────┘
```

**API 进程**：接收 HTTP 请求 → 鉴权/校验 → 同步写核心数据 → 发布 MQ 事件 → 快速返回

**Worker 进程**：消费 MQ 事件 → 异步落库 / 更新计数 / 更新热榜 / 失效缓存

**降级策略**：Redis / RabbitMQ 不可用时自动降级为直写 MySQL，保证核心功能可用

## 核心功能

### API 接口（全部 POST 方法）

| 模块 | 接口 | 说明 |
|------|------|------|
| **账号** | register / login / changePassword / rename / logout | 注册登录、改密改名、JWT 鉴权 |
| **视频** | publish / delete / getDetail / uploadVideo / uploadCover | 视频 CRUD、文件上传（mp4 200MB / 图片 10MB） |
| **点赞** | like / unlike / isLiked / listMyLikedVideos | MQ 优先 + 同步降级、事务内幂等 |
| **评论** | publish / delete / listAll | MQ 优先 + 同步降级、分页缓存 |
| **关注** | follow / unfollow / getAllFollowers / getAllVloggers | DB 优先 + MQ 通知下游 |
| **Feed** | listLatest / listLikesCount / listByPopularity / listByFollowing | 多策略 Feed 流 |

## 简历亮点

### 🚀 三级缓存架构（L1 本地 → L2 Redis → L3 MySQL）

- L1 `go-cache` 本地缓存（TTL 3s）拦截最热数据，L2 Redis（TTL 5min/1h）分担读压力，L3 MySQL 兜底
- `singleflight` 防缓存击穿（4 处），分布式锁防并发回源
- 压测结果：`video/getDetail` 吞吐量 **24,546 req/s**，P95 **6.7ms**

### 📨 事件驱动 + 最终一致性

- **Outbox 模式**：视频发布事务写 Video + OutboxMsg，Poller 轮询投递 MQ，保证 DB 与 MQ 最终一致
- **MQ 降级**：Redis/RabbitMQ 不可用时自动降级直写 MySQL，核心功能不受影响
- **幂等消费**：Worker 事务内 `LikeIgnoreDuplicateInTx` + `ON DUPLICATE KEY`，重复投递无副作用

### 🔄 推拉结合 Feed 流

- 普通用户发视频 → **推模式**：FanoutWorker 扇出到活跃粉丝收件箱（inbox ZSET）
- 大V（≥10000 粉丝）发视频 → **拉模式**：读时从大V发件箱实时合并，避免扇出风暴
- **k-way merge**（堆大小=K，O(N log K)）+ 大V公平采样 + 概率裁剪（1%，参考 Stream-Framework）
- 活跃度过滤：只推给 3 天内登录过的粉丝，僵尸粉走拉路径兜底

### 🛡️ 高可用设计

- **熔断器**：sony/gobreaker v2 保护所有 Redis 调用，连续 5 次失败触发 Open，10s 后 HalfOpen 探测
- **限流**：固定窗口（登录/注册 10/min/IP）+ 滑动窗口（点赞 30/min、评论 10/min、关注 20/min），Lua 脚本原子执行
- **分布式锁**：SETNX + 随机 token + Lua 解锁（GET + compare + DEL 原子操作，防误删）
- **热榜滑动窗口**：按分钟分桶 ZSET，`ZUNIONSTORE` 按时间衰减加权合并 60 窗口（decay=0.95）

### 📊 可观测性

- Prometheus 指标：HTTP QPS/延迟、Redis 操作计数/延迟、MQ 消息量、熔断器状态、限流拒绝
- Grafana Dashboard：8 面板可视化，自动 Provisioning
- pprof 性能分析：独立端口（API :6060 / Worker :6061）

## 压测数据

| 场景 | 并发 | 吞吐量 | P50 | P95 | P99 | 成功率 |
|------|------|--------|-----|-----|-----|--------|
| comment/listAll | 200 | 20,220 req/s | 4.9ms | 12.7ms | 34.5ms | 100% |
| video/getDetail | 200 | 24,546 req/s | 2.9ms | 6.7ms | 39.7ms | 100% |
| comment/listAll | 500 | 14,422 req/s | 10.3ms | 35.1ms | 89.1ms | 100% |

> 优化项：评论分页缓存 → 吞吐 +246%、P95 -56%；MySQL 连接池调优 → P95 -96%

## 项目结构

```
feedsystem_video_go/
├── backend_repeat/              # 后端 Go 服务
│   ├── cmd/                     # 入口（API + Worker）
│   ├── internal/
│   │   ├── account/             # 账号模块
│   │   ├── video/               # 视频/点赞/评论模块
│   │   ├── social/              # 关注关系模块
│   │   ├── feed/                # Feed 流模块
│   │   ├── worker/              # MQ 消费者（5 个 Worker）
│   │   ├── middleware/          # JWT · Redis · RabbitMQ · 限流
│   │   └── observability/       # Prometheus 指标 + pprof
│   └── Dockerfile               # 多阶段构建（api + worker）
├── frontend/                    # Vue 3 前端应用
├── configs/                     # Prometheus + Grafana 配置
├── docs/                        # 设计文档 + 压测报告
├── docker-compose.yml           # 8 服务一键编排
└── test/postman.json            # Postman 测试集合
```

## 文档

- [技术学习文档](docs/backend-study-guide.md) — 7 章，含代码引用和自检题
- [模拟面试 Q&A](docs/backend-interview-qa.md) — 10 维度 60+ 题
- [压测报告](docs/stress-test.md) — 完整压测数据
- [优化报告](docs/optimization-report.md) — 优化方案与对比
- [瓶颈分析](docs/bottleneck-analysis.md) — 性能瓶颈定位
- [可观测性与优化](docs/observability-and-optimization.md) — 合并版（面试用）

## License

MIT
