# 可观测性、压力测试与性能优化

> 项目：Go 后端短视频 Feed 流系统
> 时间：2026-05-25
> 技术栈：Prometheus + Grafana + hey 压测工具

---

## 一、Prometheus + Grafana 接入

### 1.1 架构

```
Gin 请求 → MetricsMiddleware（自动采集）→ Handler
                  ↓
         Prometheus 指标注册表
                  ↓
         Prometheus (:9090) ← 每 15s scrape /metrics
                  ↓
         Grafana (:3001) → 8 面板 Dashboard
```

### 1.2 指标清单

| 指标名 | 类型 | 标签 | 采集方式 |
|--------|------|------|----------|
| `feedsystem_http_requests_total` | Counter | method, path, status_code | Gin middleware 自动 |
| `feedsystem_http_request_duration_seconds` | Histogram | method, path | Gin middleware 自动 |
| `feedsystem_redis_operations_total` | Counter | operation, status | cache.go + zset.go + set.go 手动埋点（status: success/error/miss） |
| `feedsystem_redis_operation_duration_seconds` | Histogram | operation, status | cache.go + zset.go + set.go 手动埋点 |
| `feedsystem_mq_messages_published_total` | Counter | exchange, routing_key | PublishJSON 手动埋点 |
| `feedsystem_mq_messages_consumed_total` | Counter | queue | 所有 Worker handleDelivery 手动（IncrConsumed） |
| `feedsystem_circuit_breaker_state_changes_total` | Counter | to_state | breaker.go OnStateChange |
| `feedsystem_circuit_breaker_rejections_total` | Counter | — | breaker.go Execute |
| `feedsystem_ratelimit_rejections_total` | Counter | limiter, prefix | ratelimit 中间件 |

### 1.3 Gin Middleware 实现

```go
// internal/observability/middleware.go
func MetricsMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        start := time.Now()
        c.Next()
        duration := time.Since(start).Seconds()
        path := c.FullPath()          // 路由模板，避免 label 爆炸
        method := c.Request.Method
        statusCode := strconv.Itoa(c.Writer.Status())

        HTTPRequestsTotal.With(prometheus.Labels{
            "method": method, "path": path, "status_code": statusCode,
        }).Inc()
        HTTPRequestDuration.With(prometheus.Labels{
            "method": method, "path": path,
        }).Observe(duration)
    }
}
```

**关键设计**：用 `c.FullPath()` 而非 `c.Request.URL.Path`，避免因请求参数不同产生无数 label（如 `/video/getDetail/123` 和 `/video/getDetail/456` 会合并为 `/video/getDetail`）。

### 1.4 Docker Compose 集成

```yaml
prometheus:
  image: prom/prometheus:latest
  ports: ["9090:9090"]
  volumes:
    - ./configs/prometheus.yml:/etc/prometheus/prometheus.yml:ro
  command: ["--config.file=/etc/prometheus/prometheus.yml"]

grafana:
  image: grafana/grafana:latest
  ports: ["3001:3000"]
  environment:
    GF_SECURITY_ADMIN_USER: admin
    GF_SECURITY_ADMIN_PASSWORD: admin123
  volumes:
    - ./configs/grafana/provisioning:/etc/grafana/provisioning:ro
```

### 1.5 Grafana Dashboard 面板

1. HTTP QPS by path（每接口请求量）
2. HTTP QPS by status code（按状态码分组）
3. HTTP P50/P95/P99 延迟
4. HTTP P95 延迟 by path
5. Redis 操作 QPS
6. Redis 操作 P95 延迟
7. MQ 消息发布量
8. MQ 消息消费量

---

## 二、压力测试

### 2.1 测试工具与方法

- **工具**：`hey`（Go 写的 HTTP 压测工具）
- **方法**：用 curl 脚本模拟真实 API 调用（注册、登录、发布视频、点赞、评论、关注），再用 hey 做高并发压测
- **数据采集**：hey 原始数据 + Prometheus 持续监控

### 2.2 测试矩阵

| 组 | 场景 | 目标接口 | 并发数 | 总请求数 | 类型 |
|----|------|---------|--------|---------|------|
| 1 | 读-高并发 | `/comment/listAll` | 200 | 20,000 | 读（Redis 缓存） |
| 2 | 读-高并发 | `/video/getDetail` | 200 | 20,000 | 读（三级缓存） |
| 3 | 读-极端并发 | `/comment/listAll` | 500 | 50,000 | 读（Redis 缓存） |
| 4 | 读-混合并行 | 3 接口并行 | 200×3 | 15,000 | 读（混合） |
| 5 | 写-限流验证 | `/like/like` + `/comment/publish` | 100 | 2,000×2 | 写（MQ + 限流） |

### 2.3 核心数据

**单接口高并发 (200c, 20000n)**：

| 接口 | 吞吐量 | P50 | P95 | P99 | 成功率 |
|------|--------|-----|-----|-----|--------|
| comment/listAll | 20,220 req/s | 4.9ms | 12.7ms | 34.5ms | 100% |
| video/getDetail | 24,546 req/s | 2.9ms | 6.7ms | 39.7ms | 100% |

**极端并发 (500c, 50000n)**：

| 接口 | 吞吐量 | P50 | P95 | P99 | 成功率 |
|------|--------|-----|-----|-----|--------|
| comment/listAll | 14,422 req/s | 10.3ms | 35.1ms | 89.1ms | 100% |

**混合并行 (200×3, 15000n)**：

| 接口 | 吞吐量 | P50 | P95 | P99 | 成功率 |
|------|--------|-----|-----|-----|--------|
| comment/listAll | 10,392 req/s | 2.2ms | 11.5ms | 338ms | 100% |
| video/getDetail | 6,387 req/s | 1.6ms | 6.9ms | 670ms | 100% |

**写操作限流验证 (100c, 2000n)**：

| 接口 | 200 成功 | 429 限流 | 说明 |
|------|---------|---------|------|
| /like/like | 30 | 1,970 | 限流 30/min/用户，正确触发 |
| /comment/publish | 10 | 1,990 | 限流 10/min/用户，正确触发 |

### 2.4 Prometheus 全局指标

| 指标 | 值 |
|------|-----|
| HTTP 总 QPS | 368.5 |
| HTTP P50 | 0.89ms |
| HTTP P95 | 8.7ms |
| HTTP P99 | 19.7ms |
| Redis GET QPS | 294.8 |
| Redis GET P95 | 8.8ms |
| 缓存命中率 | ~80% |
| 500 错误 | 0 |

---

## 三、瓶颈分析

### 3.1 核心发现

| 接口 | P95 (50 并发) | P95 (100 并发) | 背后数据源 |
|------|-------------|---------------|-----------|
| `/feed/listLatest` | 1.1ms | 1.1ms | Redis ZSET + L1 缓存 + singleflight |
| `/comment/listAll` | 24.0ms | 29.1ms | MySQL 直查，无缓存 |
| `/video/getDetail` | 4.8ms | 4.8ms | 三级缓存（L1 → L2 → L3） |

**结论：有 Redis 缓存的接口延迟低且平稳，纯 MySQL 查询的接口延迟高且随并发增长。**

### 3.2 逐层瓶颈

**瓶颈 1：comment/listAll — 无缓存 + 双次 MySQL 查询**

```
GetAll()
  → videoRepo.IsExist()    // 第 1 次 MySQL（冗余）
  → repo.GetAllComments()  // 第 2 次 MySQL
```

P95 是 feed 接口的 22 倍。

**瓶颈 2：MySQL 连接池未调优**

GORM 默认配置：MaxOpenConns=0（无限制）、MaxIdleConns=2、ConnMaxLifetime=0。高并发下连接等待严重，P99 跳到 136ms。

**瓶颈 3：video/getDetail L1 TTL 过短**

L1 默认 TTL 只有 3 秒（回写时 5 秒），混合压测下频繁失效导致穿透到 L2/L3。

### 3.3 瓶颈优先级

| 优先级 | 瓶颈 | 影响范围 | 修复难度 | 预期收益 |
|--------|------|---------|---------|---------|
| P0 | comment/listAll 无缓存 | 评论查询 | 低 | P95 从 24ms 降到 <5ms |
| P0 | MySQL 连接池未配置 | 全部 DB 操作 | 低 | P99 从 136ms 降到 <50ms |
| P1 | IsExist 冗余查询 | comment 读写 | 低 | 每请求减少 1 次 MySQL |

---

## 四、性能优化

### 4.1 优化 1：comment/listAll 加 Redis 分页缓存

#### 方案迭代

**第一版：整体缓存（技术分析后否决）**

```
缓存 Key:   comment:list:video:{videoID}
缓存 Value:  该视频下所有评论的 JSON 数组（~50KB）
```

三个问题：
1. **大 Value 序列化开销**：500 条评论 = 50KB JSON，序列化/反序列化 CPU 开销大
2. **缓存失效雪崩**：一条新评论 → 整个 key DEL → 高并发下全部穿透到 MySQL
3. **全量数据传输**：前端只需要 20 条，API 返回 500 条

**第二版：分页缓存（最终方案）**

```
缓存 Key:   comment:list:video:{videoID}:page:{page}:size:{pageSize}
缓存 Value:  单页评论（最多 20 条，约 2KB）
TTL:         30 秒
失效时机:    评论发布/删除时主动 DEL 第 1 页
```

| 维度 | 整体缓存 | 分页缓存 |
|------|---------|---------|
| Value 大小 | 50KB | 2KB（25 倍缩小） |
| 序列化开销 | 高 | 低 |
| 缓存失效代价 | 全量回查 500 条 | 单页回查 20 条 |
| 并发穿透风险 | 高 | 低（各页独立失效） |

#### 代码改动

```go
// comment_service.go — 核心逻辑
func (s *CommentService) GetAll(ctx context.Context, videoID uint, page, pageSize int) ([]Comment, error) {
    cacheKey := fmt.Sprintf("comment:list:video:%d:page:%d:size:%d", videoID, page, pageSize)
    // 1. 尝试 Redis 缓存
    if data, err := s.cache.GetBytes(ctx, cacheKey); err == nil {
        var comments []Comment
        if json.Unmarshal(data, &comments) == nil {
            return comments, nil
        }
    }
    // 2. 缓存未命中，查 MySQL（去掉冗余 IsExist 检查）
    comments, _ := s.repo.GetCommentsByPage(ctx, videoID, offset, pageSize)
    // 3. 序列化并回写缓存
    if data, err := json.Marshal(comments); err == nil {
        s.cache.SetBytes(ctx, cacheKey, data, 30*time.Second)
    }
    return comments, nil
}
```

### 4.2 优化 2：MySQL 连接池调优

```go
// db.go
sqlDB.SetMaxOpenConns(100)               // 最大连接数（MySQL 默认 151，留 51 给系统）
sqlDB.SetMaxIdleConns(25)                // 空闲连接数（最大连接的 1/4）
sqlDB.SetConnMaxLifetime(5 * time.Minute) // 连接最大存活时间
sqlDB.SetConnMaxIdleTime(3 * time.Minute) // 空闲连接最大存活时间
```

### 4.3 优化效果

```
                    优化前                        优化后
                    (100c, 5000n)                 (200c, 20000n)
                    ─────────────                 ──────────────
comment/listAll     5,846 req/s                   20,220 req/s  (+246%)
                    P50: 11.4ms                   P50: 4.9ms    (-57%)
                    P95: 29.1ms                   P95: 12.7ms   (-56%)
                    P99: 135.8ms                  P99: 34.5ms   (-75%)

video/getDetail     P95: 189.4ms (混合)           P95: 6.7ms    (-96%)

Redis 缓存          未使用                        294.8 QPS (GET)
                                                  缓存命中率 ~80%

极限压测            —                             500c, 50000n, 零错误
```

### 4.4 面试话术

> "我接入 Prometheus + Grafana 后对服务做了压测，发现 comment/listAll 的 P95 延迟是 feed 接口的 22 倍。通过 Prometheus 指标和源码分析，定位到两个瓶颈：一是 comment 每次请求打 2 次 MySQL 且无缓存，二是 MySQL 连接池用的默认配置。
>
> 缓存设计上我经历了一个迭代过程。一开始我想用整体缓存，把整个评论列表缓存起来，但分析后发现三个问题：500 条评论的 JSON 有 50KB，序列化开销大；缓存失效时全量回查 MySQL，高并发写入下反而更差；前端只需要 20 条但返回了全部数据。所以我改成了分页缓存，Key 里带上 page 和 size 参数，Value 从 50KB 降到 2KB，每次失效只影响单页。
>
> 另外配置了 MySQL 连接池参数 MaxOpenConns=100、MaxIdleConns=25。优化后在 200 并发下 comment/listAll 吞吐量从 5,846 提升到 20,220 req/s，P95 从 29ms 降到 12.7ms。500 并发 5 万请求零错误。Redis 缓存命中率约 80%，Prometheus 监控显示全链路 P95 稳定在 8.7ms。"

---

## 五、系统韧性优化

### 5.1 Redis 熔断器

**问题**：Redis 网络抖动时，每个请求等 50ms 超时 → 连接池打满 → MySQL 雪崩。

**方案**：引入 `sony/gobreaker` v2 实现三态熔断器，包裹所有 Redis 调用（cache.go: GetBytes/SetBytes/Del/ZincrBy/Expire, zset.go: ZAdd/ZRevRangeByScore/ZRevRange/ZUnionStoreWithWeights/ZRemRangeByRank/ZRangeWithScores/ZRevRangeWithScores/ZCard/MGet/Exists, set.go: SAdd/SRem/SMembers）。

| 参数 | 值 | 含义 |
|------|-----|------|
| ConsecutiveFailures | 5 | 连续 5 次失败触发熔断 |
| Timeout | 10s | 熔断后 10s 进入半开探测 |
| MaxRequests | 1 | 半开状态放行 1 个探测请求 |
| Interval | 60s | Closed 状态滚动窗口（60s 无失败则清零） |

**关键设计**：
1. `redis.Nil`（缓存未命中）不计入失败，因为它是正常的回源信号
2. `IsBreakerOpen()` 方法供 Pipeline 调用方（fanoutworker、socialworker、feed/service、sliding_window）在执行前快速判断，避免在 Redis 已知不可用时阻塞等待超时
3. Worker（outboxworker、fanoutworker）收到 `ErrBreakerOpen` 时 sleep 1s 再 Nack-requeue，避免空转热循环
4. `ListLatest` 检测到 `ErrBreakerOpen` 时调用 `listLatestFromDB` 直查 MySQL，Feed 流功能不中断

**新增 Prometheus 指标**：
- `feedsystem_circuit_breaker_state_changes_total{to_state}` — 状态切换次数
- `feedsystem_circuit_breaker_rejections_total` — 熔断期间拒绝数
- `feedsystem_redis_operations_total{operation, status}` — 所有 Redis 操作计数（status: success/error/miss）
- `feedsystem_redis_operation_duration_seconds{operation, status}` — 所有 Redis 操作延迟

**代码位置**：`backend_repeat/internal/middleware/redis/breaker.go`

### 5.2 滑动窗口限流

**问题**：固定窗口限流存在"边界突刺"，窗口交界处瞬时流量可达 2× 限流值。

**方案**：基于 Redis ZSET + Lua 脚本实现滑动窗口限流，与固定窗口并存，按业务选择。

```lua
-- 原子执行：移除过期 → 计数 → 条件写入
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)
if count < limit then
    redis.call('ZADD', key, now, member)
    redis.call('PEXPIRE', key, window)
    return 0  -- 允许
end
return 1  -- 拒绝
```

**选型对比**：

| 维度 | 固定窗口 | 滑动窗口 |
|------|----------|----------|
| 边界突刺 | 有 | 无 |
| 内存 | O(1) | O(limit) |
| 延迟 | ~0.2ms | ~0.5ms |
| 适用 | 登录/注册 | 点赞/评论 |

**新增 Prometheus 指标**：`feedsystem_ratelimit_rejections_total{limiter, prefix}`

**代码位置**：`backend_repeat/internal/middleware/ratelimit/sliding_window.go`

### 5.3 新增指标完整清单

| 指标 | 类型 | 标签 | 说明 |
|------|------|------|------|
| `feedsystem_circuit_breaker_state_changes_total` | Counter | to_state | 熔断器状态切换 |
| `feedsystem_circuit_breaker_rejections_total` | Counter | — | 熔断拒绝数 |
| `feedsystem_ratelimit_rejections_total` | Counter | limiter, prefix | 限流拒绝数 |

### 5.4 面试话术（系统韧性部分）

> "性能优化做完后我发现还有一个隐患：Redis 抖动时系统会级联超时。之前的降级只是判 nil，对'连得上但响应慢'的场景无能为力。
>
> 我引入了 sony/gobreaker 做熔断器，连续 5 次 Redis 调用失败后自动熔断，10 秒后放 1 个探测请求验证恢复。关键设计是 redis.Nil 不计入失败——缓存未命中是正常回源，不是故障。
>
> 限流方面，原来的固定窗口有边界突刺问题，我补了一套基于 ZSET 的滑动窗口限流，用 Lua 脚本保证原子性。两套限流并存，登录用固定窗口（简单），点赞用滑动窗口（严格）。
>
> 两个优化都接了 Prometheus 指标，Grafana 上能看到熔断频率和限流拒绝率。"
