# 瓶颈分析报告

> 基于压力测试数据 + 源码分析
> 2026-05-25

## 1. 核心发现

压力测试暴露了一个关键对比：

| 接口 | 并发 50 的 P95 | 并发 100 的 P95 | 背后数据源 |
|------|---------------|----------------|-----------|
| `/feed/listLatest` | **1.1ms** | **1.1ms** | Redis ZSET + L1 缓存 + singleflight |
| `/comment/listAll` | **24.0ms** | **29.1ms** | MySQL 直查，无缓存 |
| `/video/getDetail` | **4.8ms** | **4.8ms** | 三级缓存（L1 → L2 → L3） |

**结论：有 Redis 缓存的接口延迟低且平稳，纯 MySQL 查询的接口延迟高且随并发增长。**

## 2. 逐层瓶颈分析

### 瓶颈 1: comment/listAll — 无缓存 + 双次 MySQL 查询

**代码路径** (`internal/video/comment_service.go`):

```
GetAll()
  → videoRepo.IsExist(ctx, videoID)    // 第 1 次 MySQL 查询：SELECT id FROM videos WHERE id = ?
  → repo.GetAllComments(ctx, videoID)  // 第 2 次 MySQL 查询：SELECT * FROM comments WHERE video_id = ? ORDER BY created_at ASC
```

**问题**：
- 每次请求执行 **2 次 MySQL 查询**，无任何缓存
- `IsExist` 检查视频存在性是冗余的 —— 如果视频不存在，`GetAllComments` 会返回空列表，语义上没有区别
- `GetAllComments` 查询 `WHERE video_id = ? ORDER BY created_at asc`，如果 Comment 表没有 `(video_id, created_at)` 联合索引，会走 filesort
- 对比 feed/listLatest（Redis ZSET + L1 go-cache + singleflight），comment 的实现停留在最原始的直查模式

**量化影响**：

| 指标 | feed/listLatest | comment/listAll | 差距 |
|------|----------------|-----------------|------|
| P50 | 0.7ms | 7.6ms | **10x** |
| P95 | 1.1ms | 24.0ms | **22x** |
| P99 | 26.4ms | 134.0ms | **5x** |

### 瓶颈 2: MySQL 连接池未调优

**代码** (`internal/db/db.go`):

```go
db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
// 没有调用 db.DB().SetMaxOpenConns() / SetMaxIdleConns()
```

**问题**：
- GORM 默认使用 Go 的 `database/sql` 默认连接池配置：
  - `MaxOpenConns`: 0（无限制）
  - `MaxIdleConns`: 2
  - `ConnMaxLifetime`: 0（永不过期）
- 无限制的连接数在高并发下会打爆 MySQL 的 `max_connections`（默认 151）
- `MaxIdleConns=2` 意味着大多数请求需要新建连接，TCP 握手开销大
- `ConnMaxLifetime=0` 意味着连接永不过期，可能导致使用过期连接

**量化影响**：
- 100 并发压测时 P99 跳到 136ms，其中大部分时间花在等待 MySQL 连接
- 混合负载下 video/getDetail 的 P95 从 4.8ms 飙到 189ms，说明连接池成为共享瓶颈

### 瓶颈 3: video/getDetail 缓存命中率不稳定

**代码路径** (`internal/video/video_service.go`):

```
GetDetail()
  → L1 本地缓存 (go-cache, TTL 3-5s)
  → L2 Redis 缓存 (STRING, TTL 5min)
  → L3 MySQL
  → singleflight 防击穿（合并同 key 并发请求）
```

**问题**：
- 三级缓存架构设计很好，但 L1 TTL 只有 3-5 秒
- 在混合压测中，L1 频繁失效导致大量请求穿透到 L2 Redis
- 当 Redis 也 miss 时（冷数据），所有请求打到 MySQL
- 虽然有 singleflight，但 singleflight 只在并发请求同时 miss 时生效，不同时间点的 miss 仍会各自查 MySQL

**量化影响**：
- 单独压测 video/getDetail 时 P95 = 4.8ms（L1/L2 命中率高）
- 混合压测时 P95 = 189ms（L1 失效 + 连接池竞争）

### 瓶颈 4: account/login bcrypt 计算密集

**延迟数据**：

| 接口 | P95 |
|------|-----|
| `/account/register` | 97.5ms |
| `/account/login` | 97.5ms |
| 其他写接口 | 23.9ms (publish) |

**问题**：
- bcrypt 哈希/验证是 CPU 密集型操作，默认 cost=10 需要 ~100ms
- 这是 bcrypt 的设计特性（故意慢以防暴力破解），不是 bug
- 但在高并发登录场景下，100ms/请求意味着单核只能支撑 10 QPS 的登录

**影响范围**：
- 仅影响 `/account/login` 和 `/account/register`
- 其他接口不受影响

### 瓶颈 5: MQ Worker 消费埋点不完整（已修复 ✅）

**问题**：
- ~~只有 LikeWorker 加了 `IncrConsumed()` 埋点~~
- ~~CommentWorker、SocialWorker、PopularityWorker、FanoutWorker 没有消费埋点~~
- ~~导致 Prometheus 采集不到消费速率，无法观测 MQ 积压情况~~

**现状**：所有 6 个 Worker（LikeWorker、CommentWorker、SocialWorker、PopularityWorker、FanoutWorker、Timeline Consumer）均已添加 `rabbitmq.IncrConsumed()` 埋点。

**影响**：
- 无法在 Grafana 中看到"发布 vs 消费"的对比图
- 生产环境中如果消费速率 < 发布速率，消息会积压，但当前无法提前预警

## 3. 瓶颈优先级排序

| 优先级 | 瓶颈 | 影响范围 | 修复难度 | 预期收益 |
|--------|------|---------|---------|---------|
| **P0** | comment/listAll 无缓存 | 评论查询接口 | 低 | P95 从 24ms 降到 <5ms |
| **P0** | MySQL 连接池未配置 | 全部 DB 操作 | 低 | P99 从 136ms 降到 <50ms |
| **P1** | Comment 表缺联合索引 | 评论查询 | 低 | 查询耗时减少 30-50% |
| **P1** | IsExist 冗余查询 | comment 读写操作 | 低 | 每请求减少 1 次 MySQL 调用 |
| **P2** | video/getDetail L1 TTL 过短 | 视频详情 | 中 | 缓存命中率提升 |
| **P2** | Worker 消费埋点不完整 | MQ 监控 | 低 | ✅ 已修复 |
| **P3** | bcrypt 成本 | 登录/注册 | 高（安全权衡） | 需评估安全风险 |

## 4. 优化方案

### 方案 1: comment/listAll 加 Redis 缓存（P0）

在 `CommentService.GetAll` 中加 Redis 缓存层：

```
查询流程改为:
  → Redis GET comment:list:video:{videoID}  (TTL 30s)
  → miss 时查 MySQL 并回写 Redis
  → 评论发布/删除时 DEL 对应缓存 key
```

预期收益：comment/listAll P95 从 24ms 降到 <5ms，与 feed 接口持平。

### 方案 2: MySQL 连接池配置（P0）

在 `db.go` 中添加连接池配置：

```go
sqlDB, _ := db.DB()
sqlDB.SetMaxOpenConns(100)        // 最大连接数
sqlDB.SetMaxIdleConns(25)         // 空闲连接数
sqlDB.SetConnMaxLifetime(5 * time.Minute)  // 连接最大存活时间
sqlDB.SetConnMaxIdleTime(3 * time.Minute)  // 空闲连接最大存活时间
```

预期收益：消除连接等待，P99 从 136ms 降到 <50ms。

### 方案 3: Comment 表加联合索引（P1）

```sql
CREATE INDEX idx_comment_video_created ON comments(video_id, created_at);
```

当前查询 `WHERE video_id = ? ORDER BY created_at asc` 如果只有 `video_id` 单列索引，MySQL 需要 filesort。联合索引可以避免排序。

### 方案 4: 去掉 comment GetAll 的 IsExist 检查（P1）

`GetAllComments` 在 video_id 不存在时返回空列表，语义上与 IsExist=false 等价。去掉 IsExist 可以减少一次 MySQL 查询。

### 方案 5: Worker 消费埋点补全（P2）（已修复 ✅）

~~在 CommentWorker、SocialWorker、PopularityWorker、FanoutWorker 的 `handleDelivery` 中加 `rabbitmq.IncrConsumed(w.queue)`，与 LikeWorker 保持一致。~~

已在所有 Worker 的 `handleDelivery` 中添加 `rabbitmq.IncrConsumed(w.queue)`。

## 5. 优化后的预期指标

| 接口 | 当前 P95 | 优化后 P95 | 优化手段 |
|------|---------|-----------|---------|
| `/comment/listAll` | 24ms | <5ms | Redis 缓存 + 联合索引 + 去掉 IsExist |
| `/video/getDetail` (混合负载) | 189ms | <20ms | 连接池调优 |
| `/account/login` | 97ms | 97ms | 不优化（bcrypt 安全设计） |
| P99 (全局) | 136ms | <50ms | 连接池调优 |
| 最大吞吐量 | 5,846 req/s | 8,000+ req/s | 缓存 + 连接池 |
