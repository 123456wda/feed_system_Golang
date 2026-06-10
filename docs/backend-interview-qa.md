# Go 后端短视频 Feed 系统 -- 模拟面试 Q&A

---

## 1. 项目介绍话术（2 分钟版）

**面试官：介绍一下你这个项目？**

好的，我做的这是一个短视频信息流后端系统。整体架构是 API 进程加 Worker 进程的双进程模型，API 负责接收请求和返回结果，Worker 负责异步消费消息做持久化。

技术栈是 Go + Gin 做 HTTP 框架，GORM 操作 MySQL，Redis 做缓存和分布式锁，RabbitMQ 做消息队列，JWT 做认证。

核心功能有五个：视频发布、点赞评论、关注关系、Feed 流浏览、热度排行。

技术亮点方面，我重点做了几个设计：

第一是**三级缓存**，L1 本地缓存（go-cache，3-5 秒 TTL）到 L2 Redis（1 小时）到 L3 MySQL，用 singleflight 防击穿，热点数据可以做到亚毫秒响应。

第二是**Outbox 模式**，视频发布时在一个事务里同时写 video 表和 outbox 表，然后由轮询器异步投递到 MQ，保证"写 DB 和发 MQ"的最终一致性。

第三是**冷热分离**，Feed 流用 Redis ZSET 存最新 1000 条热数据，超出部分走 MySQL 冷查询，中间有个 watermark 机制自动判断走哪条路径。

第四是**幂等消费**，点赞/关注的 Worker 遇到 MySQL 1062 唯一键冲突直接静默忽略，配合 At-least-once 投递保证不丢不重。

第五是**MQ 降级**，MQ 不可用时自动切到同步 MySQL 事务，核心功能不受影响。

第六是**推拉结合**，普通用户（粉丝 < 10000）发布视频时通过 FanoutMQ 推送到粉丝收件箱，大 V 不推送，读取时从发件箱拉取，k-way merge 归并排序，读取路径 Redis RTT 不随大V数量增长。

---

## 2. Redis 面试题（11 题）

### Q1: 缓存穿透、击穿、雪崩分别是什么？你的项目怎么解决的？

**参考答案：**

这三个是经典的缓存问题。

缓存穿透是查一个根本不存在的数据，缓存里没有，数据库里也没有，请求每次都打到数据库。我们项目用了一个 EMPTY_DB 短路标记，就是查完数据库发现是空的，也往缓存里写一个特殊值，下次请求直接返回，不再穿透到 DB。

缓存击穿是一个热点 key 过期了，大量并发请求同时涌进来打到数据库。项目中用了 singleflight 和 Redis 分布式锁两种方式防击穿，单机部署下两者效果一样，用两种技术是为了丰富技术栈。

singleflight 有 4 处调用：`GetVideoByIDs` 的 L3 回源（`sf:entity:{id}`）、`ListLatest` 的 ZSET 为空时重建（`sf:fallback:feed:global_timeline_rebuild`）、冷查询路径（`sf:cold:listLatest:...`）、以及热数据不够时的冷热衔接补尾（`sf:stitch:listLatest:...`）。

`GetDetail` 用分布式锁：Redis 缓存 miss 时先 `Lock`，拿到锁的请求去查 DB 并回写缓存，没拿到锁的请求轮询等待缓存回填（20ms × 5 次）。

缓存雪崩是大量 key 同时过期，数据库压力骤增。我们通过不同 TTL 来错开过期时间：L1 本地缓存 3-5 秒，L2 Redis 1 小时到 24 小时，热度窗口 2 小时。这样不会同时大面积失效。

**代码关联：**
- `internal/feed/service.go:GetVideoByIDs` -- 三级缓存 + singleflight `sf:entity:{id}` 防穿透
- `internal/feed/service.go:ListLatest` -- EMPTY_DB 短路返回 + singleflight `sf:fallback:feed:global_timeline_rebuild` 防并发重建
- `internal/feed/service.go:ListLatest` -- 冷查询 singleflight `sf:cold:listLatest:{limit}:{reqTime}`
- `internal/feed/service.go:ListLatest` -- 冷热衔接 singleflight `sf:stitch:listLatest:{remain}:{cursor}`
- `internal/video/video_service.go:GetDetail` -- 分布式锁防击穿（Lock + 轮询等待）

**追问：**
- singleflight 和分布式锁的区别是什么？各自适合什么场景？
- 如果 EMPTY_DB 对应的数据后来被创建了怎么办？
- 缓存雪崩除了随机 TTL 还有什么方案？

---

### Q2: Redis 分布式锁怎么实现的？为什么解锁要用 Lua？

**参考答案：**

加锁用的是 `SET key random_token NX PX ttl`，NX 保证只有 key 不存在时才能设置成功，PX 设置毫秒级过期防止死锁。random_token 是一个随机字符串，用来标识锁的持有者。

解锁必须用 Lua 脚本，因为要保证"GET + 比较 + DEL"这三步是原子的。如果不原子，可能出现这种情况：我 GET 发现 token 是我的，准备 DEL，但在这之间我的锁过期了，别人拿到了锁，结果我把别人的锁删了。Lua 脚本里先 GET 比较 token 是否一致，一致才 DEL，不一致直接返回 0，整个过程是原子的。

**代码关联：**
- `internal/middleware/redis/redis.go:Lock` -- SetNX 加锁
- `internal/middleware/redis/redis.go:UnLock` -- Lua 脚本解锁
- `internal/middleware/redis/redis.go:unlockScript` -- Lua 脚本定义（GET+比较+条件 DEL）

**追问：**
- 如果 Redis 主从切换，锁丢失怎么办？（Redlock）
- 分布式锁的续期问题怎么解决？（看门狗机制）
- 除了 Redis，还有哪些实现分布式锁的方案？

---

### Q3: ZSET 在你项目中的三个应用场景？

**参考答案：**

第一个是**时间线**，key 是 `feed:global_timeline`，score 是视频创建时间的毫秒时间戳，member 是视频 ID。查询时用 ZREVRANGEBYSCORE 按时间倒序分页，ZREMRANGEBYRANK 裁剪只保留最新 1000 条。

第二个是**热度窗口**，key 是 `hot:video:1m:YYYYMMDDHHmm`，每分钟一个窗口，score 是互动量（点赞、评论触发 ZINCRBY），2 小时自动过期。

第三个是**合并热榜**，查询热度排行时用 ZUNIONSTORE 把过去 60 个 1 分钟窗口按时间衰减加权合并（decay=0.95，越老权重越小），生成一个临时 ZSET，然后 ZREVRANGE 分页取 Top N。

第四个是**推拉结合的收件箱和发件箱**，`inbox:{userID}` 存粉丝收到的推送视频（cap 500），`user_videos:{authorID}` 存作者最近发布的视频（cap 50），score 都是创建时间毫秒时间戳。读取时从 inbox 取推路径数据，从大 V 的 user_videos 取拉路径数据，k-way merge 归并。

**代码关联：**
- `internal/worker/outboxworker.go:StartConsumer` -- ZADD + ZREMRANGEBYRANK 写时间线
- `internal/video/popularity_cache.go:UpdatePopularityCache` -- ZINCRBY 更新热度窗口
- `internal/feed/service.go:ListByPopularity` -- ZUNIONSTORE 合并 60 窗口
- `internal/worker/fanoutworker.go:process` -- ZADD 写 inbox + user_videos
- `internal/feed/service.go:ListByFollowing` -- ZREVRANGEWITHSCORES 读 inbox + user_videos

**追问：**
- ZSET 的时间复杂度是多少？ZREVRANGEBYSCORE 和 ZINCRBY 各是多少？
- 如果视频量远超 1000 条，只保留最新 1000 条会不会丢数据？
- ZUNIONSTORE 合并 60 个 key 的性能开销大吗？
- inbox 的 cap 是多少？概率裁剪是什么？

---

### Q4: 三级缓存架构怎么设计的？为什么需要 L1？

**参考答案：**

三级缓存是 L1 本地缓存到 L2 Redis 到 L3 MySQL。

L1 用的是 go-cache 库，存在进程内存里，默认 TTL 3 秒，回写时用 5 秒。L2 是 Redis，TTL 1 小时。L3 是 MySQL，源数据。

查询时先查 L1，命中直接返回；没命中查 L2，命中就写回 L1 然后返回；L2 也没命中就查 L3，查到后异步写回 L2，同步写回 L1。

为什么需要 L1？主要是减少网络开销。Redis 虽然快，但每次查询还是要走一次网络往返，大概 0.5 到 1 毫秒。L1 在进程内存里，亚毫秒就能返回。对于热点数据（比如首页 feed 流反复访问的视频详情），L1 能把 Redis 的 QPS 压力降低很多。

L1 的 TTL 只有 3-5 秒，所以数据一致性的影响很小，最多延迟几秒感知到变化。

**代码关联：**
- `internal/feed/service.go:GetVideoByIDs` -- 三级缓存查询逻辑（L1 miss -> L2 miss -> L3）
- `internal/feed/service.go:NewFeedService` -- go-cache 初始化，`cache.New(3*time.Second, 5*time.Second)`
- L2 异步回写：`go func()` 异步写 Redis（1h TTL）

**追问：**
- L1 缓存的一致性怎么保证？3-5 秒 TTL 够吗？
- go-cache 的过期清理机制是什么？
- 如果有多个 API 实例，L1 缓存之间怎么同步？

---

### Q5: singleflight 和分布式锁分别在什么场景用？

**参考答案：**

singleflight 是 Go 标准库 `golang.org/x/sync/singleflight` 提供的，同一个 key 的并发请求只有一个会真正执行，其他请求等待并共享结果。它只能在单个进程内去重。

分布式锁用 Redis SETNX 实现，可以跨多个 API 实例去重。

在我们项目里，两种都用了——singleflight 用在 FeedService（4 处），分布式锁用在 VideoService.GetDetail。单机部署下两者效果一样，用两种技术是为了丰富技术栈。

singleflight 有 4 处调用：`GetVideoByIDs` 的 L3 回源（`sf:entity:{id}`）、`ListLatest` 的 ZSET 重建（`sf:fallback:feed:global_timeline_rebuild`）、冷查询（`sf:cold:listLatest:...`）、冷热衔接补尾（`sf:stitch:listLatest:...`）。

`GetDetail` 用分布式锁：Redis 缓存 miss 时 `Lock`（SETNX，2s TTL），拿到锁的查 DB 回写缓存，没拿到的轮询等待（20ms × 5 次）。

如果要部署多实例，理论上分布式锁可以跨实例去重，singleflight 只能在单进程内去重。但对本项目来说这不是选型的考量因素。

**代码关联：**
- `internal/feed/service.go:GetVideoByIDs` -- singleflight `sf:entity:{id}`
- `internal/feed/service.go:ListLatest` -- singleflight `sf:fallback:feed:global_timeline_rebuild`、`sf:cold:listLatest:...`、`sf:stitch:listLatest:...`
- `internal/video/video_service.go:GetDetail` -- 分布式锁防击穿（Lock + 轮询等待）
- `internal/middleware/redis/redis.go:Lock` -- 分布式锁工具

**追问：**
- singleflight 的 shared 返回值是什么含义？
- 如果分布式锁的持有者宕机了怎么办？
- 这两种机制能不能组合使用？

---

### Q6: Redis 和 MySQL 双写一致性怎么保证？

**参考答案：**

严格的一致性很难做到，我们项目采用的是**最终一致性**策略，不同场景用不同方式：

视频详情缓存：采用 async write-back + TTL。查询 miss 时从 MySQL 读，然后异步写回 Redis，TTL 1 小时。更新时通过 `UpdatePopularityCache` 主动 DEL 缓存 key，下次读就会重新从 DB 加载。

热度缓存：event-driven invalidation。每次点赞/评论触发 `UpdatePopularityCache`，先 DEL 详情缓存 key，再 ZINCRBY 更新分钟窗口。

时间线：Outbox 模式保证最终一致性。视频发布时在一个事务里写 video 表和 outbox 表，轮询器投递到 MQ，消费者写入 Redis ZSET。

总结就是：读路径靠缓存 + TTL 兜底，写路径靠主动失效 + 事件驱动更新。

**代码关联：**
- `internal/video/popularity_cache.go:UpdatePopularityCache` -- DEL 详情缓存 + ZINCRBY 更新热度
- `internal/video/video_repo.go:PublishVideo` -- Outbox 事务写入
- `internal/worker/outboxworker.go:StartOutboxPoller` -- 轮询投递 MQ
- `internal/feed/service.go:GetVideoByIDs` -- 异步回写 Redis（第 499 行）

**追问：**
- 如果 DEL 缓存和更新 DB 之间有延迟怎么办？
- 为什么不直接用 Canal 监听 binlog？
- 什么场景适合用 Cache Aside 模式？

---

### Q7: Lua 脚本在你项目中的两个用途？

**参考答案：**

第一个是**限流**，Lua 脚本把 INCR 和 PEXPIRE 合成一个原子操作。逻辑是：先 INCR key，如果 count 等于 1（说明是新 key），就设置 PEXPIRE 过期时间。这样保证了计数和过期时间设置是原子的，不会出现 INCR 成功但 PEXPIRE 没执行的情况。

第二个是**分布式锁解锁**，Lua 脚本把 GET + token 比较 + DEL 合成原子操作。先 GET key 的值，和传入的 token 比较，一致才 DEL，不一致返回 0。这样防止了释放别人锁的问题。

为什么必须用 Lua？因为 Redis 的单线程模型下，Lua 脚本内的所有命令是原子执行的，中间不会被其他命令插入。

**代码关联：**
- `internal/middleware/redis/redis.go:incrementWithExpireScript` -- 限流 Lua（INCR+PEXPIRE）
- `internal/middleware/redis/redis.go:unlockScript` -- 解锁 Lua（GET+比较+DEL）
- `internal/middleware/ratelimit/ratelimit.go:Limit` -- 限流中间件调用

**追问：**
- Lua 脚本在 Redis 里执行时会阻塞其他命令吗？
- 如果 Lua 脚本执行时间过长会怎样？
- 除了 Lua，Redis 还有什么方式保证原子性？

---

### Q8: Redis 为什么这么快？

**参考答案：**

三个主要原因：

第一，**单线程事件循环**。Redis 的核心处理逻辑是单线程的，不存在线程切换和锁竞争的开销。虽然 Redis 6.0 之后引入了多线程 IO，但核心命令执行还是单线程。

第二，**内存存储**。数据都在内存里，读写速度是纳秒级的，比磁盘 IO 快几个数量级。

第三，**IO 多路复用**。用 epoll/kqueue 实现了单线程同时处理大量连接，不会因为一个连接的 IO 操作阻塞其他连接。

另外 Redis 的数据结构也做了优化，比如 ZSET 用的是跳表 + 哈希表的组合，范围查询和单点查询都很快。

**代码关联：**
- 本项目大量使用 Redis 操作：`internal/middleware/redis/` 目录

**追问：**
- Redis 6.0 的多线程 IO 是怎么工作的？
- 单线程怎么利用多核 CPU？
- Redis 和 Memcached 的区别？

---

### Q9: Redis 持久化 RDB vs AOF？

**参考答案：**

RDB 是快照方式，每隔一段时间把内存里的数据dump到磁盘上生成一个 .rdb 文件。优点是恢复快、文件小，缺点是可能丢失最后一次快照到宕机之间的数据。

AOF 是日志追加方式，每次写命令都追加到 .aof 文件里。优点是数据更安全（可以配置每秒同步一次），缺点是文件大、恢复慢。

生产环境一般两者都开，AOF 用来保数据，RDB 用来快速恢复。

对我们项目来说，Redis 里的数据（时间线 ZSET、热度窗口、缓存）都可以从 MySQL 重建，所以 RDB 够用，丢一点数据影响不大。热度窗口本身 2 小时就过期了，更不需要持久化。

**代码关联：**
- `internal/feed/service.go:ListLatest` -- ZSET 为空时从 MySQL 重建

**追问：**
- AOF 重写是什么？为什么要重写？
- 混合持久化是怎么回事？
- 如果 Redis 数据量很大，RDB fork 子进程会不会影响性能？

---

### Q10: 限流的 Lua 脚本为什么必须原子执行？

**参考答案：**

限流脚本的逻辑是：先 INCR 自增计数器，如果是新 key（count 等于 1），再设置 PEXPIRE 过期时间。

如果 INCR 和 PEXPIRE 分成两条命令执行，中间如果进程 crash 或者网络断了，就会出现：key 已经被 INCR 了，但没有设置过期时间。这个 key 就会永远存在于 Redis 里，对应的 IP 或用户就被永久限流了。

用 Lua 脚本保证原子性，要么两条命令都执行成功，要么都不执行，不会出现中间状态。

另外 Lua 脚本在 Redis 里是串行执行的，不存在并发竞争问题。

**代码关联：**
- `internal/middleware/redis/redis.go:incrementWithExpireScript` -- Lua 脚本定义
- `internal/middleware/redis/redis.go:IncrementWithExpire` -- 调用入口

**追问：**
- 如果 Redis 限流 key 被手动删了会怎样？
- 固定窗口限流有什么缺点？怎么改进？
- 令牌桶和漏桶的区别？

---

## 3. 消息队列面试题（9 题）

### Q1: 为什么要用消息队列？

**参考答案：**

三个原因：解耦、异步、削峰。

解耦：API 进程只负责投递消息，不关心谁来消费、怎么持久化。Worker 进程负责消费消息写 MySQL，两边独立演进。

异步：点赞请求来了，API 只需要把消息发到 MQ 就可以返回了，不用等 MySQL 写完。用户体验上响应更快。

削峰：如果突然有大量点赞请求，MQ 充当缓冲区，Worker 按自己的速度消费，不会把 MySQL 打挂。

我们项目用了 5 个 Topic Exchange：social、like、comment、popularity、timeline，分别对应不同的业务事件。

**代码关联：**
- `internal/middleware/rabbitmq/likeMQ.go` -- LikeMQ 生产者
- `internal/middleware/rabbitmq/socialMQ.go` -- SocialMQ 生产者
- `internal/worker/likeworker.go` -- LikeWorker 消费者

**追问：**
- 消息队列会带来什么问题？
- 什么场景不适合用消息队列？
- RabbitMQ 和 Kafka 的区别？

---

### Q2: 如何保证消息不丢失？

**参考答案：**

从三个环节保证：

**Producer 端**：发送消息时设置 delivery mode 为 Persistent，消息会持久化到磁盘。

**Broker 端**：Exchange 和 Queue 都声明为 durable，即使 RabbitMQ 重启也不会丢失。

**Consumer 端**：关闭 autoAck，用 manual Ack/Nack。处理成功才 Ack，处理失败 Nack 并 requeue，消息重新入队下次再试。

我们项目 LikeWorker 就是这样做的：`Ch.Consume` 时 autoAck 设为 false，处理成功调 `d.Ack(false)`，失败调 `d.Nack(false, true)` 让消息重新入队。

**代码关联：**
- `internal/worker/likeworker.go:Run` -- autoAck=false 手动确认
- `internal/worker/likeworker.go:handleDelivery` -- Ack/Nack 逻辑
- `internal/middleware/rabbitmq/rabbitmq.go` -- durable exchange/queue 声明

**追问：**
- 如果消息一直 Nack 重试怎么办？（死信队列）
- RabbitMQ 的镜像队列是什么？
- 消息持久化会有什么性能影响？

---

### Q3: 如何保证消息不重复消费？

**参考答案：**

我们用的是**幂等消费**，核心是数据库唯一索引。

LikeWorker 处理点赞消息时，在事务内调用 `LikeIgnoreDuplicateInTx` 插入点赞记录，如果遇到 MySQL 1062 唯一键冲突错误，说明记录已存在（重复消息），直接返回 created=false，跳过计数更新。只有 created=true（真正插入了新记录）才在同一事务内更新点赞数和热度。事务保证三步操作原子执行，避免部分成功导致数据不一致。

SocialWorker 处理关注消息也是同理，catch 1062 错误静默忽略。

这个方案的前提是：业务表有唯一索引（比如 user_id + video_id 的联合唯一索引），数据库层面保证不会插入重复数据。

At-least-once 投递 + 幂等消费 = 效果上的 Exactly-once。

**代码关联：**
- `internal/worker/likeworker.go:applyLike` -- 事务内 LikeIgnoreDuplicateInTx + created 判断
- `internal/video/like_repo.go:LikeIgnoreDuplicateInTx` -- 事务内 catch 1062 错误
- `internal/worker/socialworker.go` -- 同样 catch 1062

**追问：**
- 如果业务表不适合加唯一索引怎么办？
- 除了唯一索引，还有什么实现幂等的方式？
- 消息体里带一个全局唯一 ID 做去重行不行？

---

### Q4: 什么是 Outbox 模式？

**参考答案：**

Outbox 模式解决的是"写数据库"和"发消息"不能原子的问题。你想，如果先写 DB 再发 MQ，DB 写成功了但 MQ 发失败，数据不一致；反过来先发 MQ 再写 DB，MQ 发了但 DB 写失败，消息已经出去了也收不回来。

Outbox 的做法是：在一个数据库事务里，同时写业务表和 outbox 表。然后有一个独立的轮询器，不断从 outbox 表里取 status=pending 的记录，投递到 MQ，成功后删除这条记录。

我们项目里，`PublishVideo` 就是在一个事务里同时创建 video 记录和 outbox_msg 记录。然后 `StartOutboxPoller` 每秒轮询一次，把 pending 的记录**双投递**：先投递 TimelineMQ（写全局时间线），成功后投递 FanoutMQ（推送到粉丝收件箱），两者都成功才删除 outbox 记录。

如果轮询器投递失败，记录还在 outbox 表里，下次轮询会重试。如果轮询器投递成功但删除 outbox 记录时失败了，下次轮询会重复投递，但消费者是幂等的（ZADD 同一个 video_id 会覆盖），所以没问题。

**代码关联：**
- `internal/video/video_repo.go:PublishVideo` -- 事务写 video + outbox_msgs
- `internal/worker/outboxworker.go:StartOutboxPoller` -- 双投递逻辑（TimelineMQ + FanoutMQ）
- `internal/worker/outboxworker.go:StartConsumer` -- 消费 TimelineMQ 写 ZSET
- `internal/middleware/rabbitmq/fanoutMQ.go` -- FanoutMQ 生产者

**追问：**
- Outbox 轮询的延迟怎么控制？
- 和 CDC（如 Debezium）比有什么优缺点？
- outbox 表的记录一直删不掉怎么办？

---

### Q5: MQ 不可用时系统怎么降级？

**参考答案：**

我们项目的核心设计是**MQ 优先，同步降级**。

以点赞为例，`LikeService.Like()` 会先尝试投递 LikeMQ 和 PopularityMQ。如果两条 MQ 都投递成功，直接返回，Worker 异步消费。

如果 LikeMQ 投递失败（likeMQSent=false），就走降级路径：同步执行 MySQL 事务，直接在 API 进程里写点赞记录、更新点赞数和热度。

如果 PopularityMQ 投递失败（popularityMQSent=false），就直接调用 `UpdatePopularityCache` 更新 Redis 热度缓存。

所以 MQ 是"尽力而为"的，降级路径保证核心数据不丢。MQ 恢复后，后续请求又会走 MQ 优先路径。

**代码关联：**
- `internal/video/like_service.go:Like` -- likeMQSent/popularityMQSent 双标志位降级
- `internal/video/like_service.go:Unlike` -- 同样的降级逻辑
- `internal/video/popularity_cache.go:UpdatePopularityCache` -- 降级时直接更新 Redis

**追问：**
- 降级到同步路径后，性能会受多大影响？
- 如果 MySQL 也挂了怎么办？
- 怎么监控 MQ 是否可用？

---

### Q6: RabbitMQ Exchange 类型有哪些？

**参考答案：**

RabbitMQ 有四种 Exchange 类型：

**Direct**：精确匹配 routing key。消息的 routing key 和队列绑定的 routing key 完全一致才投递。

**Fanout**：广播模式，忽略 routing key，消息会投递到所有绑定的队列。

**Topic**：支持通配符匹配。routing key 用点号分隔，`*` 匹配一个词，`#` 匹配零个或多个词。比如 `video.like.#` 可以匹配 `video.like.create` 和 `video.like.delete`。

**Headers**：基于消息 header 属性匹配，很少用。

我们项目用的是 Topic Exchange，有 5 个：social、like、comment、popularity、timeline。Topic 类型比较灵活，后续如果要加新的事件类型，只需要加新的 routing key 绑定就行。

**代码关联：**
- `internal/middleware/rabbitmq/likeMQ.go` -- LikeMQ Topic Exchange 声明
- `internal/middleware/rabbitmq/socialMQ.go` -- SocialMQ Topic Exchange 声明

**追问：**
- Topic 和 Direct 的性能差别大吗？
- 如果一个 Exchange 绑定了很多队列，性能会下降吗？
- RabbitMQ 的死信队列是什么？

---

### Q7: At-least-once vs At-most-once vs Exactly-once？

**参考答案：**

**At-most-once**：消息最多投递一次，可能丢消息。就是 fire and forget，Producer 发出去就不管了，Consumer 收到就处理，收不到就算了。性能最好，但不安全。

**At-least-once**：消息至少投递一次，不丢但可能重复。Producer 发送后等 Broker 确认，没收到确认就重发。Consumer 处理失败会 requeue。我们项目用的就是这种。

**Exactly-once**：消息恰好投递一次，不丢不重。这在分布式系统里很难实现，需要 Producer 端幂等发送 + Broker 端去重 + Consumer 端幂等处理。

实际项目中，一般用 At-least-once + Consumer 端幂等来达到效果上的 Exactly-once。我们项目的 LikeWorker 就是这样：At-least-once 投递 + 唯一索引幂等消费。

**代码关联：**
- `internal/worker/likeworker.go:Run` -- At-least-once（Nack requeue）
- `internal/worker/likeworker.go:applyLike` -- 事务内幂等消费（LikeIgnoreDuplicateInTx catch 1062）

**追问：**
- Kafka 的 Exactly-once 是怎么实现的？
- 幂等 Producer 是什么？
- 事务消息是怎么回事？

---

### Q8: Outbox 轮询 vs CDC？

**参考答案：**

**Outbox 轮询**：定时从 outbox 表 SELECT status='pending' 的记录，投递到 MQ。优点是实现简单，不需要额外依赖，用现有的 MySQL 就行。缺点是有延迟（取决于轮询间隔），而且频繁 SELECT 对数据库有一定压力。

**CDC（Change Data Capture）**：监听数据库的 binlog，实时捕获数据变更事件。典型工具是 Debezium。优点是延迟极低（毫秒级），不侵入业务代码。缺点是需要额外部署 Debezium + Kafka Connect 等基础设施，运维复杂度高。

我们项目用的是 Outbox 轮询，每秒轮询一次。对于短视频系统来说，1 秒的延迟完全可以接受。如果后续流量大了、对延迟要求更高，可以考虑迁移到 CDC。

**代码关联：**
- `internal/worker/outboxworker.go:StartOutboxPoller` -- 每秒轮询，LIMIT 100

**追问：**
- Outbox 表膨胀怎么办？
- Debezium 监听 binlog 会不会影响 MySQL 性能？
- 除了 Debezium 还有哪些 CDC 工具？

---

### Q9: FanoutMQ 和 TimelineMQ 的关系？

**参考答案：**

它们复用同一个 Topic Exchange（`video.timeline.events`），但使用不同的 routing key 和独立队列。TimelineMQ 的 binding key 是 `video.timeline.publish`，队列是 `video.timeline.update.queue`，负责写全局时间线 ZSET。FanoutMQ 的 binding key 是 `video.timeline.fanout`，队列是 `video.timeline.fanout.queue`，负责推送到粉丝收件箱。

OutboxPoller 双投递时，先投递 TimelineMQ 再投递 FanoutMQ。两者都成功才删除 outbox 记录。如果 FanoutMQ 投递失败，下次轮询会重试。FanoutWorker 消费 fanout 消息后，写 `user_videos:{authorID}` 发件箱，然后判断作者是否大 V，普通用户才 fanout 到粉丝 inbox。

**代码关联：**
- `internal/middleware/rabbitmq/fanoutMQ.go` -- FanoutMQ 生产者
- `internal/middleware/rabbitmq/timelineMQ.go` -- TimelineMQ binding key 改为 `video.timeline.publish`
- `internal/worker/fanoutworker.go:process` -- 消费 fanout 消息

**追问：**
- 为什么复用同一个 Exchange 而不是新建一个？
- 如果 FanoutMQ 投递成功但 TimelineMQ 失败怎么办？
- FanoutWorker 启动的条件是什么？

---

## 4. Feed 流设计面试题（10 题）

### Q1: 推模型、拉模型、推拉结合？

**参考答案：**

**推模型**：用户发布视频时，主动把视频 ID 写入所有粉丝的收件箱。优点是读取快，直接从收件箱取就行。缺点是大 V 有几百万粉丝，发一条视频要写几百万次，写放大严重。

**拉模型**：用户刷 feed 时，实时查询自己关注的人的最新视频。优点是发布快，不用写收件箱。缺点是读取慢，关注了很多人的话每次都要查很多数据。

**推拉结合**：普通用户（粉丝 < 10000）用推模型，发布时通过 FanoutMQ 推送到粉丝收件箱（inbox ZSET）；大 V（粉丝 >= 10000）不推送，读取时从发件箱（user_videos ZSET）拉取。读取时两路数据 k-way merge 归并排序，对用户无感知。

我们项目实现了**推拉结合**。FanoutWorker 消费 fanout 消息后，写 `user_videos:{authorID}` 发件箱，然后判断作者粉丝数。普通用户过滤活跃粉丝（3 天内登录过，`user:active:{id}` TTL 72h）后分批 fanout 到粉丝 inbox（Pipeline 批量 ZADD，1% 概率裁剪）。大 V 只写发件箱，不 fanout。僵尸粉不推，走拉路径兜底。读取时并行读 inbox 和 bigV 列表，提前终止判断，拉取配额跳过冷大V（`newestAt <= inboxOldest`），Pipeline 批量拉取大V发件箱，k-way merge 归并。

**代码关联：**
- `internal/worker/fanoutworker.go:process` -- 推逻辑（写 inbox + user_videos）
- `internal/feed/service.go:ListByFollowing` -- 拉逻辑（并行读取 + k-way merge）
- `internal/worker/socialworker.go:onFollow` -- 关注回填 + bigV SET 维护

**追问：**
- 什么算"大 V"？粉丝数阈值怎么定？
- 推拉结合怎么处理取关后收件箱的清理？
- k-way merge 的时间复杂度？

---

### Q2: 冷热分离怎么做的？

**参考答案：**

核心思路是：热数据放 Redis，冷数据放 MySQL，中间用 watermark 分界。

具体实现：Redis ZSET `feed:global_timeline` 只保留最新 1000 条视频（ZREMRANGEBYRANK 裁剪）。ZSET 里最老那条的 score 就是 watermark。

查询时，比较请求时间戳和 watermark：
- 请求时间 > watermark：走 Redis 热查询，用 ZREVRANGEBYSCORE 分页
- 请求时间 <= watermark：走 MySQL 冷查询

还有一个边界情况：热数据不足 limit 条时，需要补冷尾。就是从 MySQL 里查剩下的条数，拼接到热数据后面。冷数据查完后不回写 ZSET，防止冷数据污染热点时间线。

**代码关联：**
- `internal/feed/service.go:ListLatest` -- 冷热分离主逻辑
- 第 99 行 `reqTime <= watermark` 判断走冷/热路径
- 第 141 行 `len(baseVideos) < limit` 补冷尾逻辑
- `internal/worker/outboxworker.go:StartConsumer` -- ZREMRANGEBYRANK 裁剪保留 1000 条

**追问：**
- watermark 会不会变化？什么时候变化？
- 1000 条够用吗？怎么确定这个值？
- 冷数据回写 ZSET 会有什么问题？

---

### Q3: 游标分页 vs offset 分页？

**参考答案：**

**Offset 分页**：`LIMIT 20 OFFSET 1000`。数据库需要扫描前 1020 条，丢掉前 1000 条，返回 20 条。偏移量越大越慢。而且如果中间有数据插入或删除，会导致翻页时数据错位或重复。

**游标分页**：用上一页最后一条数据的某个字段作为游标，`WHERE create_time < ? LIMIT 20`。始终从游标位置开始扫描，O(log N + limit)，不会因为偏移量大而变慢，也不会错位。

我们项目有三种游标：

1. **时间游标**：`ListLatest` 用 create_time 毫秒时间戳做游标
2. **双字段游标**：`ListLikesCount` 用 (likes_count, id) 做游标，处理 likes_count 相同的情况
3. **三字段游标**：`ListByPopularity` 用 (popularity, create_time, id) 做游标，处理多字段并列

**代码关联：**
- `internal/feed/service.go:ListLatest` -- 时间游标（NextTime）
- `internal/feed/service.go:ListLikesCount` -- 双字段游标（NextLikesCountBefore + NextIDBefore）
- `internal/feed/service.go:ListByPopularity` -- 三字段游标 + as_of/offset 稳定分页

**追问：**
- 游标分页的缺点是什么？
- 为什么需要双字段/三字段游标？
- 游标分页怎么做"跳到第 N 页"？

---

### Q4: 热度排行怎么实现的？

**参考答案：**

热度排行分两步：**实时更新**和**查询合并**。

实时更新：每次点赞、评论等互动操作，触发 `UpdatePopularityCache`，用 ZINCRBY 给对应视频在当前分钟窗口的 ZSET 里加分。窗口 key 格式是 `hot:video:1m:YYYYMMDDHHmm`，2 小时自动过期。

查询合并：查询热度排行时，用 ZUNIONSTORE 把过去 60 个分钟窗口按时间衰减加权合并（decay=0.95，最新窗口权重 1.0，59 分钟前权重 0.046），生成一个临时 ZSET。然后 ZREVRANGE 分页取 Top N。

为了分页稳定，用 as_of + offset 方案：as_of 固定为查询时的分钟时间戳，offset 递增。这样同一批次的分页请求都基于同一个快照，不会因为数据变化导致重复或遗漏。

如果 Redis 不可用，降级到 MySQL 三字段游标分页：`(popularity, create_time, id)`。

**代码关联：**
- `internal/video/popularity_cache.go:UpdatePopularityCache` -- ZINCRBY 更新窗口
- `internal/feed/service.go:ListByPopularity` -- ZUNIONSTORE 按时间衰减加权合并 + as_of/offset 分页
- `internal/middleware/redis/zset.go:ZUnionStoreWithWeights` -- ZUNIONSTORE 加权封装

**追问：**
- 为什么用 60 个窗口而不是一个大的 ZSET？
- ZUNIONSTORE 的时间复杂度？
- 热度衰减怎么实现？

---

### Q5: 三字段游标怎么处理并列？

**参考答案：**

热度排行可能有并列情况：两个视频的 popularity 和 create_time 都一样。这时候需要第三个字段来打破平局，我们用的是 video id。

三字段游标的 WHERE 条件是：

```
(popularity < ?)
OR (popularity = ? AND create_time < ?)
OR (popularity = ? AND create_time = ? AND id < ?)
```

这个条件的含义是：找出所有"严格小于"当前游标的记录。三个字段依次比较，只要有一个字段严格小于就满足条件。

为什么用 id 做第三字段？因为 id 是自增的，天然唯一，保证不会出现三个字段完全相同的情况。

**代码关联：**
- `internal/feed/service.go:ListByPopularity` -- 三字段游标参数
- `internal/feed/repo.go:ListByPopularity` -- WHERE 条件构造

**追问：**
- 如果不用 id 做第三字段，还有什么选择？
- 这个 WHERE 条件能不能用索引？
- 四字段游标有没有必要？

---

### Q6: 为什么冷数据不回写 ZSET？

**参考答案：**

两个原因：

第一，**防止污染热点时间线**。ZSET 只保留最新 1000 条，是为了让热查询高效。如果把冷数据也写回去，ZSET 会膨胀，热查询的性能就下降了。而且冷数据是历史数据，用户很少翻到那么老的内容，没必要占用热数据的空间。

第二，**冷数据流量极小**。能翻到冷数据的用户本身就很少，冷查询走 MySQL 的开销可以接受。即使不缓存，MySQL 的索引查询对于小流量也够用。

所以设计上就是：热数据用 Redis 加速，冷数据直接走 MySQL，互不干扰。

**代码关联：**
- `internal/feed/service.go:ListLatest` 第 110 行注释：`// 不回写 ZSET，防止冷数据污染热点时间线`

**追问：**
- 如果用户频繁翻到冷数据怎么办？
- 冷数据有没有必要单独建一个 ZSET？
- 怎么监控冷热比例？

---

### Q7: SoftJWTAuth 的设计思路？

**参考答案：**

Feed 流的接口（listLatest、listLikesCount、listByPopularity）是公共接口，未登录的用户也能看。但登录用户需要看到个性化数据，比如每条视频的 is_liked 状态。

所以设计了 SoftJWTAuth：有 token 就解析身份，把 accountID 写入 Gin 的 Context；没有 token 也放行，accountID 为 0。

后续 `buildFeedVideos` 会用 accountID 去批量查 is_liked。如果 accountID 是 0，就跳过查询，is_liked 全部返回 false。

对比硬认证 `JWTAuth`：没有 token 或 token 无效直接返回 401。

**代码关联：**
- `internal/middleware/jwt/jwt.go:SoftJWTAuth` -- 软认证中间件
- `internal/middleware/jwt/jwt.go:JWTAuth` -- 硬认证中间件
- `internal/http/router.go` 第 186 行 -- feedGroup 使用 SoftJWTAuth
- `internal/feed/service.go:buildFeedVideos` -- 用 viewerAccountID 查 is_liked

**追问：**
- SoftJWTAuth 的 token 无效时怎么处理？
- 如果 SoftJWTAuth 也要做限流，按什么 key 限流？
- 还有哪些场景适合用 SoftJWTAuth？

---

### Q8: 如何支持亿级用户？

**参考答案：**

分几层来看：

**存储层**：MySQL 单库扛不住，需要分库分表。按 user_id 哈希分片，每片几百万用户。视频表按 video_id 分片。

**缓存层**：Redis 单机内存有限，需要 Redis Cluster 做分片。每个节点负责一部分 key slot。热度窗口的 ZSET 可以按窗口时间分片。

**应用层**：API 水平扩容，前面加负载均衡（Nginx / K8s Ingress）。每个 API 实例无状态，JWT 认证不需要 session 共享。

**消息队列**：RabbitMQ 单机扛不住可以做集群 + 镜像队列，或者换 Kafka。

**视频分发**：视频文件走 CDN，不经过后端。

**Feed 流策略**：推拉结合。普通用户用推模型（发布时写收件箱），大 V 用拉模型（读取时实时查）。这样避免大 V 的写放大。

**代码关联：**
- 当前架构在 `cmd/main.go` 和 `cmd/worker/main.go`

**追问：**
- 分库分表后怎么做全局唯一 ID？
- 分库分表后怎么做跨分片查询？
- Redis Cluster 的数据迁移怎么做？

---

### Q9: 推拉结合的读取路径怎么优化的？

**参考答案：**

七个优化点：

第一是**粉丝活跃度过滤**，推路径只推给 3 天内登录过的粉丝。登录时写 `user:active:{id}` TTL 72h，FanoutWorker 用 Pipeline 批量 EXISTS 过滤。僵尸粉不推收件箱，减少写放大，走拉路径兜底。

第二是**并行读取**，inbox 和 bigV 列表通过 goroutine 并行查询，串行 2 次 RTT 压缩为并行 1 次。

第三是**公平提前终止**，Pipeline 批量查所有大V的最新 1 条时间戳，取 max(newestAt) 与 inboxOldest 比较。只有所有大V的最新内容都比 inbox 最老还旧时才跳过拉取。旧逻辑只采样 1 个大V，如果那个大V恰好没发新内容，其他活跃大V的内容就丢了。

第四是**拉取配额跳过冷大V**，分配拉取配额时，跳过 `newestAt <= inboxOldest` 的大V。这些大V的最新内容比 inbox 最老还旧，拉出来也排不到前面，省掉无意义的 Redis 查询。

第五是**活跃度优先拉取**，按 newestAt 降序排序大V，从最活跃的开始分配配额。needed=limit-len(pushStream)，每个大V最多取 min(remaining, 10) 条，配额用完即止。旧逻辑所有大V uniform 取同样数量，活跃大V和沉默大V待遇相同。

第六是**Pipeline 批量读取**，所有大V的探测+拉取合并为 1 次 Redis Pipeline 往返，RTT 从 O(N) 降为 O(1)。

第七是**真正的 k-way merge**，用 streamCursor 迭代器 + mergeHeap 实现，堆大小 = 流数量 K（非总元素数 N）。每次 pop 后从同一流 advance 取下一个入堆，O(N log K) 时间复杂度，O(K) 空间复杂度。

通过上述优化，无论关注了多少大V，读取路径 Redis 网络往返恒定约 3 次（不随大V数量增长）。

**代码关联：**
- `internal/feed/service.go:ListByFollowing` -- 并行读取 + 公平提前终止 + 活跃度优先拉取 + Pipeline
- `internal/feed/service.go:mergeAndDedup` -- 真正的 k-way merge（streamCursor + mergeHeap）
- `internal/feed/service.go:streamCursor` / `mergeHeap` / `bigVInfo` -- 数据结构

**追问：**
- 旧的单大V采样有什么问题？什么场景下会误判？
- 活跃度优先 vs 均匀分配的 trade-off？
- Pipeline 失败时怎么降级？
- 如果 inbox 为空怎么办？

---

### Q10: k-way merge 怎么实现的？

**参考答案：**

用 Go 标准库 `container/heap` 实现真正的 k-way merge。核心思想：堆大小 = 流数量 K，而非总元素数 N。

数据结构有三个：`streamCursor` 封装已排序视频流的迭代器（peek 查看当前元素、advance 推进到下一个），`mergeItem` 封装堆中的元素（video + 来源流引用），`mergeHeap` 实现 `heap.Interface` 按 CreateTime 降序排列。

算法流程：
1. 初始化：每个流取第一个元素入堆（堆大小 = K）
2. 循环：弹出堆顶（createTime 最大的），去重后加入结果
3. 从弹出元素的来源流取下一个元素入堆
4. 重复直到堆空或取够 limit 条

关键区别：旧实现把所有流的所有元素拍扁成一个大数组建堆（堆大小 = N），丢失了"每个流已排序"的结构性质。新实现维护 K 个迭代器，每次只入堆一个元素，堆大小恒为 K。

时间复杂度 O(N log K)，N 是总视频数，K 是流数量。空间复杂度 O(K + limit)，K 是堆大小，limit 是去重 map 和结果集的上限。

**代码关联：**
- `internal/feed/service.go:mergeAndDedup` -- k-way merge 入口
- `internal/feed/service.go:streamCursor` -- 流迭代器（peek/advance）
- `internal/feed/service.go:mergeItem` -- 堆元素（video + cursor 引用）
- `internal/feed/service.go:mergeHeap` -- heap.Interface 实现

**追问：**
- container/heap 是最大堆还是最小堆？怎么实现降序？
- 去重用什么数据结构？时间复杂度？
- 如果某个大V的发件箱为空怎么办？
- 为什么堆大小是 K 而不是 N？性能差异有多大？

## 5. 系统设计面试题（6 题）

### Q1: 设计一个短视频信息流系统

**参考答案：**

分四个环节：

**上传**：客户端把视频文件上传到 OSS/S3，拿到 URL。然后把视频元数据（标题、描述、封面、播放 URL）通过 API 写入 MySQL。

**发布**：API 在一个事务里写 video 表 + outbox 表（Outbox 模式）。轮询器双投递到 TimelineMQ（写全局时间线）和 FanoutMQ（推送到粉丝收件箱）。FanoutWorker 消费后写 user_videos 发件箱，普通用户 fanout 到粉丝 inbox，大 V 不 fanout。

**浏览**：客户端请求 feed 流，API 走冷热分离查询。热数据从 Redis ZSET 取，冷数据从 MySQL 取。三级缓存加速视频详情查询。游标分页返回结果。

**互动**：点赞/评论先尝试 MQ 异步写入，MQ 不可用降级同步写 MySQL。Worker 消费消息做持久化，唯一索引保证幂等。

**代码关联：**
- `cmd/main.go` -- 整体启动流程
- `internal/http/router.go` -- 路由和依赖注入
- `internal/feed/service.go` -- Feed 流查询
- `internal/video/like_service.go` -- 点赞互动

**追问：**
- 视频转码怎么做？
- 推荐算法怎么接入？
- 怎么做内容审核？

---

### Q2: 点赞数实时更新如何设计？

**参考答案：**

用户点赞后，需要实时更新三个地方：MySQL 点赞记录、MySQL likes_count 字段、Redis 热度缓存。

流程是：

1. API 收到点赞请求，尝试投递 LikeMQ + PopularityMQ
2. 两条 MQ 都成功 → API 直接返回
3. LikeMQ 失败 → API 同步执行 MySQL 事务（写点赞记录 + 更新 likes_count + 更新 popularity）
4. PopularityMQ 失败 → API 直接调 `UpdatePopularityCache` 更新 Redis

Worker 消费 LikeMQ 时：在事务内幂等插入点赞记录（LikeIgnoreDuplicateInTx catch 1062），成功才在同一事务内更新 likes_count（GREATEST 防负数）和 popularity。事务保证三步原子执行。

**代码关联：**
- `internal/video/like_service.go:Like` -- MQ 优先 + 同步降级
- `internal/worker/likeworker.go:applyLike` -- 事务内幂等消费
- `internal/video/video_repo.go:ChangeLikesCount` -- GREATEST(likes_count + ?, 0)

**追问：**
- 为什么不直接同步更新？为什么要用 MQ？
- GREATEST 防负数是什么场景？
- 点赞数的精度问题？（并发更新）

---

### Q3: 如何处理热门视频的流量突增？

**参考答案：**

四层防护：

**限流**：点赞接口用 `KeyByAccount` 限流，每用户每分钟最多 30 次。防止恶意刷赞。

**防击穿**：singleflight 合并并发请求（FeedService 4 处）+ 分布式锁保护（GetDetail），单机下效果一样。

**L1 本地缓存**：热门视频详情在进程内存里缓存 3-5 秒，减少 Redis 网络往返。

**降级**：Redis 不可用时直接查 MySQL。MQ 不可用时同步写 MySQL。

**代码关联：**
- `internal/http/router.go` 第 98 行 -- likeLimiter 限流
- `internal/feed/service.go:GetVideoByIDs` -- singleflight + L1 缓存
- `internal/video/like_service.go:Like` -- MQ 降级

**追问：**
- 限流被触发后用户看到什么？
- singleflight 有没有超时机制？
- 如果 MySQL 也扛不住怎么办？

---

### Q4: 如果流量增大 10 倍？

**参考答案：**

逐层扩容：

**API 层**：无状态，直接加机器，前面放负载均衡。Gin 框架本身性能很好，单机几万 QPS 没问题。

**缓存层**：Redis 单机升 Redis Cluster。本地缓存每个 API 实例自带，天然分布式。

**数据库层**：MySQL 读写分离（主写从读），如果还不够就分库分表。

**消息队列**：RabbitMQ 做集群 + 镜像队列。或者换 Kafka（吞吐量更高）。

**视频分发**：视频文件走 CDN，后端只处理元数据。

**Feed 流**：已实现推拉结合，可通过调整大V阈值优化 fanout 范围。

**代码关联：**
- 当前架构支持水平扩展（API 无状态 + JWT 无状态认证）

**追问：**
- 扩容时怎么做灰度发布？
- 数据库分库分表的中间件选型？
- 怎么做容量评估？

---

### Q5: CAP 理论？你的系统偏向哪个？

**参考答案：**

CAP 理论说分布式系统三选二：一致性（C）、可用性（A）、分区容忍性（P）。网络分区是不可避免的，所以实际上是 CP 或 AP 二选一。

我们系统偏向 **AP**：最终一致性 + 高可用。

具体表现：
- Redis 缓存可能和 MySQL 短暂不一致，但通过 TTL 和主动失效保证最终一致
- Outbox 模式保证消息最终投递，但有秒级延迟
- MQ 不可用时降级到同步路径，保证可用性
- 点赞计数用 GREATEST 防负数，容忍短暂的不一致

放弃的是强一致性：用户点赞后可能需要几毫秒到几秒才能在其他地方看到更新。

**代码关联：**
- `internal/video/like_service.go:Like` -- AP 设计（MQ 异步 + 降级）
- `internal/video/video_repo.go:PublishVideo` -- Outbox 最终一致性

**追问：**
- 什么场景必须用 CP？
- Raft 和 Paxos 的区别？
- 怎么监控一致性的延迟？

---

### Q6: 分布式 ID 生成方案？

**参考答案：**

我们项目目前用的是 random hex，主要用于 outbox 记录和缓存 key，不需要全局有序。

如果要生成全局唯一且有序的 ID，有几种方案：

**Snowflake**：64 位，1 位符号 + 41 位时间戳 + 10 位机器 ID + 12 位序列号。优点是时间有序、性能好、不依赖外部服务。缺点是时钟回拨会导致 ID 重复。

**UUID v7**：时间有序的 UUID，兼容 UUID 格式。不需要机器 ID 配置，但长度 128 位，索引效率不如 Snowflake。

**Leaf（美团）**：号段模式，从数据库批量申请一段 ID，用完再申请。优点是简单可靠，缺点是依赖数据库。

对于短视频系统，视频 ID 用 Snowflake 比较合适：时间有序方便按时间排序，分布式生成不依赖中心节点。

**代码关联：**
- `internal/middleware/redis/redis.go:randToken` -- 当前的随机 token 生成

**追问：**
- Snowflake 的时钟回拨怎么解决？
- 分布式 ID 的长度对数据库索引有什么影响？
- 自增 ID 在分布式环境下有什么问题？

---

## 6. Go 语言与工程面试题（8 题）

### Q1: singleflight 的原理？

**参考答案：**

singleflight 维护一个 map，key 是请求的标识，value 是一个 `call` 结构体。

当一个请求进来，如果 map 里没有这个 key，就创建一个新的 call，标记为正在进行中，然后执行实际的函数调用。其他相同 key 的请求进来时，发现 map 里已经有这个 key 了，就等待（通过 channel），不重复执行。

执行完成后，所有等待的请求共享同一个结果。`Do` 方法返回三个值：结果、错误、shared。shared=true 表示你的请求是共享别人的执行结果，不是自己执行的。

**代码关联：**
- `internal/feed/service.go:GetVideoByIDs` -- `requestGroup.Do(sfKey, func()...)`
- `internal/feed/service.go:ListLatest` -- ZSET 回源重建、冷查询、冷热衔接补尾均用 singleflight

**追问：**
- singleflight 的 map 会不会内存泄漏？
- singleflight 和 errgroup 的区别？
- 如果执行的函数 panic 了会怎样？

---

### Q2: Go 中 context 的作用？

**参考答案：**

Context 在 Go 里做三件事：

**取消传播**：父 context 取消时，所有子 context 也取消。比如 HTTP 请求超时了，所有相关的数据库查询、Redis 操作都应该取消。

**超时控制**：`context.WithTimeout` 设置截止时间，超时后 context 自动取消。我们项目 Redis 操作都设了 50ms 超时。

**值传递**：`context.WithValue` 可以在 context 里存值，比如请求 ID、用户信息。我们项目用 Gin 的 `c.Set("accountID", ...)` 存用户信息。

项目里有个细节：Redis 操作用独立的 context（`context.WithTimeout(context.Background(), 50*time.Millisecond)`），不依赖请求的 context。这样即使请求被取消了，Redis 操作也能完成，不会连带失败。

**代码关联：**
- `internal/video/popularity_cache.go:UpdatePopularityCache` -- 独立 context 超时
- `internal/feed/service.go:GetVideoByIDs` -- `cancelCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)`
- `internal/http/router.go:GracefulShutdown` -- context 控制停机超时

**追问：**
- context.WithValue 的 key 用什么类型？
- context 会不会导致内存泄漏？
- Go 1.21 的 context.AfterFunc 是什么？

---

### Q3: WaitGroup 和 Mutex 的使用场景？

**参考答案：**

**WaitGroup**：等待一组 goroutine 全部完成。调用 `wg.Add(1)` 启动 goroutine，goroutine 结束时调用 `wg.Done()`，主 goroutine 调用 `wg.Wait()` 阻塞等待。

**Mutex**：保护共享资源，同一时刻只有一个 goroutine 能访问。`mu.Lock()` 加锁，`mu.Unlock()` 解锁。

在我们项目 `GetVideoByIDs` 里两个都用了：

WaitGroup 用于并行扇出：对每个 missedL2 的 video ID 启动一个 goroutine 去查 MySQL，`wg.Wait()` 等所有 goroutine 完成。

Mutex 用于保护 videoMap：多个 goroutine 并发写入 `videoMap[id] = &safeCopy`，需要加锁防止 data race。

**代码关联：**
- `internal/feed/service.go:GetVideoByIDs` 第 480-519 行 -- WaitGroup + Mutex 并行查询

**追问：**
- WaitGroup 和 channel 的使用场景怎么选？
- sync.Map 和 Mutex+Map 的区别？
- RWMutex 什么时候用？

---

### Q4: Gin 中间件执行顺序？

**参考答案：**

Gin 中间件按照注册顺序执行。每个中间件调用 `c.Next()` 将控制权传递给下一个中间件，执行完后续中间件和 handler 后再返回。`c.Abort()` 终止链，后续中间件和 handler 不会执行。

我们项目的限流中间件就是这样：如果超过限制，调用 `c.AbortWithStatusJSON(429, ...)` 终止请求；否则调用 `c.Next()` 继续执行。

执行顺序示例（点赞接口）：
1. Logger 中间件（gin.Default 自带）
2. JWTAuth 中间件（验证 token）
3. Limit 中间件（限流检查）
4. LikeHandler.Like（业务处理）

如果 JWT 验证失败，Abort 返回 401，不会执行限流和 handler。

**代码关联：**
- `internal/middleware/ratelimit/ratelimit.go:Limit` -- c.AbortWithStatusJSON / c.Next
- `internal/middleware/jwt/jwt.go:JWTAuth` -- c.AbortWithStatusJSON / c.Next
- `internal/http/router.go` -- 中间件注册顺序

**追问：**
- c.Next() 和 c.Set() 的执行顺序？
- 怎么实现中间件的"后置逻辑"？
- Gin 的路由树是怎么组织的？

---

### Q5: GORM 事务使用方式？

**参考答案：**

GORM 推荐用 `db.Transaction(func(tx *gorm.DB) error {...})` 的方式使用事务。函数返回 nil 就自动 commit，返回 error 就自动 rollback。

我们项目做了进一步封装：`LikeRepository.Transaction` 方法接收一个 `func(tx *gorm.DB) error`，在事务内调用 `LikeInTx`、`ChangeLikesCount` 等方法时传入 tx。这样 Service 层不直接访问 repo 的 db 字段，所有数据库操作都通过 Repository 的方法完成。

关键点：事务内的操作必须用 tx 而不是原来的 db，否则不在同一个事务里。我们项目用 `db := tx; if db == nil { db = r.db }` 的模式，事务内传 tx，非事务场景传 nil 自动降级为 r.db。

**代码关联：**
- `internal/video/like_service.go:Like` 第 98 行 -- `s.likeRepo.Transaction(ctx, func(tx *gorm.DB) error {...})`
- `internal/video/video_repo.go:ChangeLikesCount` -- `db := tx; if db == nil { db = r.db }`
- `internal/video/video_repo.go:PublishVideo` -- 事务写 video + outbox

**追问：**
- GORM 事务的隔离级别怎么设置？
- 嵌套事务怎么处理？
- 大事务有什么问题？

---

### Q6: Go 错误处理哲学？

**参考答案：**

Go 的哲学是**错误是值**，显式返回和检查。不用 try-catch，用 `err != nil` 判断。

好处是：错误处理逻辑清晰可见，不会被隐藏在 catch 块里。每个可能出错的地方都要显式处理。

Go 1.13 引入了 `errors.Is` 和 `errors.As`，可以判断错误链中的特定错误类型。比如 `errors.Is(err, redis.Nil)` 判断是不是 Redis 的 key 不存在错误。

我们项目里的错误处理风格：
- 参数校验失败：返回 `errors.New("video_id and account_id are required")`
- 业务规则违反：返回 `errors.New("already followed")`
- 底层错误透传：`if err != nil { return err }`
- 特殊错误判断：`if errors.Is(err, gorm.ErrRecordNotFound) { return false, nil }`

**代码关联：**
- `internal/video/like_service.go:Like` -- 参数校验 + 错误返回
- `internal/social/service.go:Follow` -- 业务规则错误
- `internal/video/video_repo.go:IsExist` -- errors.Is 判断 ErrRecordNotFound

**追问：**
- Go 的 error wrapping 是什么？
- panic 和 error 的使用场景？
- 怎么自定义错误类型？

---

### Q7: 优雅停机怎么实现？

**参考答案：**

优雅停机的目标是：收到停止信号后，不立即关闭，而是等正在进行的请求处理完再关闭。

我们项目用 `signal.NotifyContext` 监听 SIGTERM 信号。收到信号后，调用 `http.Server.Shutdown`，它会：
1. 停止接受新连接
2. 等待所有活跃请求完成
3. 超过 5 秒超时后强制关闭

Worker 进程用 errCh 收集 goroutine 的错误，收到 context 取消信号后等待所有 goroutine 退出。

**代码关联：**
- `internal/http/router.go:GracefulShutdown` -- signal.Notify + srv.Shutdown
- `cmd/main.go` -- 启动流程和 defer 关闭

**追问：**
- 优雅停机时消息队列的消费怎么处理？
- Kubernetes 的 preStop hook 是什么？
- 怎么测试优雅停机？

---

### Q8: Dockerfile 多阶段构建的好处？

**参考答案：**

多阶段构建是把 Dockerfile 分成多个 stage：编译阶段和运行阶段。

编译阶段用完整的 Go 镜像，包含编译器、所有依赖，负责编译出二进制文件。

运行阶段用最小基础镜像（比如 alpine 或 scratch），只复制编译好的二进制进去。

好处：
1. **镜像体积小**：运行镜像只有几十 MB，不含编译器和源码
2. **安全性好**：攻击面小，没有多余的工具
3. **构建独立**：编译阶段的中间层不会进入最终镜像

我们项目有两个 target：api 和 worker，分别编译和部署。

**代码关联：**
- `backend_repeat/Dockerfile` -- 多阶段构建定义

**追问：**
- scratch 和 alpine 的区别？
- 怎么减小 Go 二进制的体积？
- Docker 的层缓存机制？

---

## 7. 认证与安全面试题（5 题）

### Q1: JWT 的组成？

**参考答案：**

JWT 由三部分组成，用点号分隔：

**Header**：声明算法和类型。我们用 HS256（HMAC-SHA256），类型是 JWT。

**Payload**：存放业务数据。我们的 Claims 包含 account_id、username，以及标准字段 exp（过期时间）、iat（签发时间）、nbf（生效时间）。

**Signature**：用 Header 指定的算法对 Header.Payload 进行签名。HS256 用密钥做 HMAC-SHA256，只有持有密钥的人才能生成和验证签名。

HS256 vs RS256：HS256 是对称算法，签名和验证用同一个密钥，适合单体应用。RS256 是非对称算法，用私钥签名、公钥验证，适合微服务（公钥可以公开分发）。

**代码关联：**
- `internal/auth/jwt.go:GenerateToken` -- 生成 JWT（HS256）
- `internal/auth/jwt.go:ParseToken` -- 解析验证 JWT
- `internal/auth/jwt.go:Claims` -- Claims 结构体定义

**追问：**
- JWT 的 Payload 能放敏感数据吗？
- JWT 的过期时间设多长合适？
- JWT 和 OAuth2 的关系？

---

### Q2: JWT vs Session？

**参考答案：**

**JWT**：无状态，token 里包含了用户信息，服务端不需要存储。每次请求带上 token，服务端解析验证就行。水平扩展友好，任何实例都能验证 token。

**Session**：有状态，服务端需要存储 session 数据（内存、Redis、数据库）。客户端只存一个 session ID。需要集中存储才能支持多实例。

JWT 的缺点是**不能主动吊销**。token 一旦签发，在过期之前一直有效。如果要实现踢人下线，需要额外的黑名单机制（比如 Redis 黑名单）。

Session 的优点是**可以主动失效**，删除 session 数据就行。

我们项目用 JWT + Redis Token 存储来实现吊销：登录时把 token 存到 Redis（key 是 `account:{id}`），每次请求比对 token 是否一致，不一致说明已被吊销。

**代码关联：**
- `internal/auth/jwt.go` -- JWT 签发和解析
- `internal/middleware/jwt/jwt.go:check` -- Redis token 比对实现吊销

**追问：**
- JWT 的续期怎么做？
- Refresh Token 是什么？
- 分布式 Session 怎么实现？

---

### Q3: 如何实现 JWT 吊销？

**参考答案：**

我们项目用的是 **Redis Token 存储**方案：

登录时，生成 JWT 后同时把 token 存到 Redis，key 是 `account:{id}`，TTL 24 小时。

每次请求时，JWTAuth 中间件先解析 token 拿到 accountID，然后从 Redis 取出该用户存储的 token，和请求中的 token 比对。如果一致，说明是最新有效的 token；如果不一致，说明已被吊销（可能是用户改密码了、或者在其他设备登录了），返回 401。

如果 Redis 不可用，降级到 MySQL：查 account 表的 token 字段做比对。

退出登录时，删除 Redis 里的 key 即可吊销。

**代码关联：**
- `internal/middleware/jwt/jwt.go:check` -- Redis 比对 + MySQL 降级
- `internal/account/service.go` -- 登录时存储 token 到 Redis

**追问：**
- 如果用户同时在多个设备登录怎么办？
- Token 黑名单和 Token 白名单的区别？
- 怎么实现"踢掉其他设备"？

---

### Q4: 什么是 TOCTOU？

**参考答案：**

TOCTOU 是 Time of Check to Time of Use 的缩写，意思是"检查时间到使用时间"之间的竞态窗口。

举个例子：两个并发请求同时点赞同一个视频。如果先检查 IsLiked 发现没点过赞，然后执行插入。但在检查和插入之间，另一个请求也检查通过了，结果两个请求都插入了，点赞数多加了 1。

我们项目里，点赞操作**不预检查**：不调 IsLiked，直接插入点赞记录，依赖唯一索引（user_id + video_id）兜底。如果重复插入，MySQL 返回 1062 错误，静默忽略。这样就消除了 TOCTOU 窗口。

但关注操作**保留预检查**：先调 IsFollowed 检查是否已关注，如果已关注返回"已经关注了"的友好提示。这里存在 TOCTOU 窗口，但 uniqueIndex 兜底。保留预检查是用户体验优先的设计取舍。

**代码关联：**
- `internal/video/like_service.go:Like` -- 不预检查，依赖唯一索引
- `internal/social/service.go:Follow` -- 保留预检查 + uniqueIndex 兜底
- `internal/worker/likeworker.go:applyLike` -- 事务内 LikeIgnoreDuplicateInTx catch 1062

**追问：**
- 唯一索引和乐观锁的区别？
- 除了唯一索引还有什么防重方式？
- 乐观锁的 version 字段怎么用？

---

### Q5: bcrypt 的原理？

**参考答案：**

bcrypt 是专门为密码哈希设计的算法，基于 Blowfish 密码算法。

特点：
1. **自带 salt**：每次哈希自动生成随机 salt，相同的密码会产生不同的哈希值，防止彩虹表攻击。
2. **计算慢**：故意设计成计算密集型，有可调节的 cost 参数。普通硬件上一次 bcrypt 大约需要 100ms。暴力破解的成本非常高。
3. **适应性**：随着硬件变快，可以增大 cost 参数来保持计算时间。

对比 MD5/SHA256：这些算法设计目标是快，不适合做密码哈希。GPU 每秒可以算几十亿次 MD5，但 bcrypt 只能算十几次。

我们项目用 bcrypt 做密码哈希，登录时对比哈希值，不存明文密码。

**代码关联：**
- `internal/account/service.go` -- bcrypt 哈希和验证

**追问：**
- bcrypt 的 cost 参数怎么选？
- argon2 和 bcrypt 哪个更好？
- 为什么不能用 SHA256 + salt 做密码哈希？

---

## 8. 场景题与开放题（7 题）

### Q1: 如果 Redis 挂了？

**参考答案：**

分场景看影响：

**缓存**：三级缓存降级，L1 本地缓存还在（3-5 秒 TTL），L3 MySQL 兜底。查询性能会下降，但功能不受影响。Feed 流的热查询会降级到 MySQL。

**防击穿**：singleflight 不依赖 Redis，进程内仍然有效。分布式锁依赖 Redis，不可用时返回 locked=false，降级到 MySQL 查询。

**限流**：`IncrementWithExpire` 在 redis client 为 nil 时返回 0，限流器放行所有请求（fail-open）。系统可用性优先，宁可放过一些恶意请求也不能把正常用户挡在外面。

**Token 吊销**：降级到 MySQL 查 account 表的 token 字段做比对。功能正常，只是多了一次数据库查询。

**代码关联：**
- `internal/middleware/redis/redis.go` -- nil check 放行
- `internal/middleware/jwt/jwt.go:check` -- MySQL 降级
- `internal/middleware/ratelimit/ratelimit.go:Limit` -- cache nil 时 c.Next()

**追问：**
- 怎么监控 Redis 是否可用？
- Redis 集群模式下单个节点挂了怎么办？
- 怎么做 Redis 的高可用？

---

### Q2: 如果 RabbitMQ 宕机？

**参考答案：**

自动降级到同步路径，核心功能不受影响。

以点赞为例：`LikeService.Like()` 尝试投递 LikeMQ，失败后 likeMQSent=false，走降级路径——API 进程直接执行 MySQL 事务写入。PopularityMQ 失败则直接调 `UpdatePopularityCache` 更新 Redis。

Outbox 轮询器会持续重试投递，MQ 恢复后自动补发积压的消息。

所以 MQ 宕机的影响是：API 响应变慢（同步写 MySQL 比异步 MQ 慢），但数据不会丢。

**代码关联：**
- `internal/video/like_service.go:Like` -- likeMQSent 降级逻辑
- `internal/worker/outboxworker.go:StartOutboxPoller` -- 持续重试
- `cmd/main.go` -- MQ 连接失败时 rbq=nil

**追问：**
- 怎么监控 MQ 是否可用？
- MQ 恢复后积压的消息怎么处理？
- 有没有可能出现 MQ 恢复后消息乱序？

---

### Q3: 限流算法的缺点？

**参考答案：**

项目中**两套限流并存**，按业务选择：

- **固定窗口**（登录/注册）：`INCR + PEXPIRE`，简单高效，低频场景够用。缺点是**窗口边界突刺**：限制每分钟 100 次，用户在第 59 秒发 100 次，第 61 秒又发 100 次，2 秒内 200 次但每个窗口都没超限。
- **滑动窗口**（点赞/评论/关注）：基于 Redis ZSET + Lua 脚本，统计过去 window 时间内的实际请求数，任意时刻严格不超 limit，无边界突刺。代价是 O(limit) 内存和 ~0.3ms 额外延迟。

其他限流算法：
1. **滑动窗口计数器**：当前窗口计数 × 权重 + 上一个窗口计数，近似滑动窗口。
2. **令牌桶**：以恒定速率产生令牌，请求消耗令牌。允许突发流量（桶里有存积令牌）。
3. **漏桶**：请求进入桶，以恒定速率流出处理。平滑流量，但不允许突发。

**代码关联：**
- `internal/middleware/ratelimit/ratelimit.go:Limit` -- 固定窗口限流（登录/注册）
- `internal/middleware/ratelimit/sliding_window.go:SlidingWindowLimit` -- 滑动窗口限流（点赞/评论/关注）
- `internal/middleware/redis/redis.go:incrementWithExpireScript` -- 固定窗口 Lua

**追问：**
- 令牌桶和漏桶的区别？
- 分布式限流怎么做？
- 限流被拒绝时返回什么状态码？
- 滑动窗口的 ZSET member 为什么需要原子计数器？

---

### Q4: 如何监控系统健康？

**参考答案：**

我们项目做了几层监控：

**pprof**：独立端口启动 pprof server，可以做 CPU profiling、内存分析、goroutine 泄漏检测。通过 `go tool pprof` 连接分析。

**Redis 健康检查**：启动时 Ping 验证连接，运行时如果操作失败会降级。

**RabbitMQ 管理界面**：可以看队列深度、消费速率、连接数。队列深度持续增长说明消费跟不上生产。

**日志**：Worker 处理失败会 log.Printf 记录错误。可以接入 ELK 做日志分析。

进一步可以加：
- Prometheus + Grafana：采集 QPS、延迟 P99、错误率
- 链路追踪（Jaeger/Zipkin）：跟踪请求在多个服务间的流转
- 告警：错误率超过阈值、延迟超过阈值时通知

**代码关联：**
- `internal/observability/pprof.go` -- pprof server
- `cmd/main.go` -- pprof 启动 + Redis Ping

**追问：**
- pprof 的 profile 类型有哪些？
- 怎么发现 goroutine 泄漏？
- Prometheus 的四种 Metric 类型？

---

### Q5: 收件箱容量怎么控制？为什么用概率裁剪？

**参考答案：**

收件箱（`inbox:{followerID}`）是一个 ZSET，上限 500 条。每次 FanoutWorker 推送视频时需要裁剪。

最简单的做法是每次写入都 `ZREMRANGEBYRANK 0 -501`，但裁剪操作的开销和 inbox 大小成正比（O(log N)），高频写入时开销不可忽略。

我参考了 Stream-Framework 的设计，用**概率裁剪**：每次写入时以 1% 的概率触发裁剪（`trimChance=0.01`）。这意味着：

- 平均每 100 次写入裁剪 1 次，裁剪开销降低 100 倍
- inbox 可能短暂超过 500 条（最多到 ~550 条），但在下次裁剪时恢复
- 这是经典的"摊还"思想：把 O(N) 的裁剪成本分摊到 100 次写入中

为什么是 1% 而不是 0.1% 或 10%？这是 Stream-Framework 在生产环境验证过的经验值。太低会导致 inbox 膨胀太久，太高就退化成每次裁剪。

**代码关联：**
- `internal/worker/fanoutworker.go:trimChance` -- 常量定义
- `internal/worker/fanoutworker.go:fanoutToFollowers` -- 概率裁剪逻辑

**追问：**
- 如果用户关注了 1000 个创作者，inbox 500 条够吗？怎么调整？
- 概率裁剪和定时裁剪相比有什么优劣？
- 取关时为什么不清理 inbox？

---

### Q6: 评论缓存从整体缓存到分页缓存的迭代过程？

**参考答案：**

最初考虑的是"整体缓存"：把整个评论列表序列化为 JSON 存进 Redis，Key 是 `comment:list:video:{id}`。

分析后发现三个问题：

第一，**Value 太大**。500 条评论的 JSON 约 50KB，序列化/反序列化开销大，Redis 内存浪费。

第二，**缓存失效代价高**。新增一条评论就要失效整个缓存，下次请求触发全量回查 MySQL。

第三，**前端只需要 20 条**。缓存了 500 条但每次只用 20 条，浪费 96% 的带宽和内存。

改为分页缓存后：

- Key 带 page+size 参数：`comment:list:video:{id}:page:{p}:size:{s}`
- Value 从 50KB 降到 2KB（25 倍缩小）
- 各页独立失效，并发穿透风险低
- 失效策略：评论发布/删除时只 DEL 第 1 页（最常被查询），其他页靠 30s TTL 自然过期

**局限**：只失效第 1 页意味着第 2+ 页可能在 30s 内看到旧数据。生产环境可用 Redis SCAN + DEL 模式匹配失效所有页，或用版本号方案（每条评论带版本，客户端判断是否需要刷新）。

**代码关联：**
- `internal/video/comment_service.go:invalidateCommentCache` -- 只删 page:1:size:20
- `docs/optimization-report.md` -- 完整迭代分析

**追问：**
- 版本号方案怎么实现？和 SCAN+DEL 相比有什么优劣？
- 如果评论量特别大（10 万条），分页缓存还能用吗？
- 缓存 key 中的 size 参数如果用户自定义，会不会导致缓存命中率低？

---

### Q7: 如果重新设计，你会改什么？

**参考答案：**

五个改进方向：

**分布式 ID**：当前用 random hex，不有序、不适合做数据库主键。改用 Snowflake，时间有序，方便按时间范围查询。

**JWT 改进**：当前 token 24 小时过期，没有 refresh 机制。改用 access token（15 分钟）+ refresh token（7 天）分离，更安全。

**限流算法**：已实现固定窗口 + 滑动窗口双策略（登录用固定窗口，点赞/评论/关注用滑动窗口）。进一步可引入令牌桶支持突发流量。

**CDC 替代 Outbox 轮询**：当前每秒轮询一次 outbox 表，有延迟且增加 DB 压力。改用 Debezium 监听 binlog，毫秒级延迟。

**Redis Cluster**：当前单机 Redis，内存和 QPS 有上限。改用 Redis Cluster 分片，支持水平扩展。

**代码关联：**
- `internal/middleware/redis/redis.go:randToken` -- 当前 random hex
- `internal/auth/jwt.go` -- 当前 JWT 无 refresh
- `internal/middleware/ratelimit/ratelimit.go` -- 固定窗口（登录/注册）
- `internal/middleware/ratelimit/sliding_window.go` -- 滑动窗口（点赞/评论/关注）
- `internal/worker/outboxworker.go` -- 当前轮询模式

**追问：**
- 这些改进的优先级怎么排？
- 改动过程中怎么保证线上服务不受影响？
- 还有什么可以优化的地方？

---

## 9. 可观测性与系统韧性面试题（7 题）

### Q1: 你们这个项目怎么发现性能问题的？讲一下完整流程。

**参考答案：**

完整链路是 **监控 → 假设 → 验证 → 优化 → 复测**，我用 comment/listAll 接口举例。

第一步**监控发现**：我接了 Prometheus + Grafana 后看 Dashboard，"HTTP P95 by path" 面板上 comment/listAll 的 P95 持续 29ms，是 feed 接口（1.1ms）的 22 倍以上。这就是异常信号。

第二步**假设瓶颈**：可能是慢 SQL、未加缓存、连接池打满，先列出候选。

第三步**验证假设**：开 pprof 看 CPU profile，发现 70% 的时间在 GORM 的 MySQL 调用；同时在 MySQL 上 `SHOW PROCESSLIST` 看到连接打到默认 2 的上限。两个瓶颈都印证了。

第四步**优化**：加 Redis 分页缓存（key 带 page+size，TTL 30s，写时主动 DEL）；调连接池 MaxOpenConns=100, MaxIdleConns=25。

第五步**复测**：再用 hey 200 并发跑 20000 请求，吞吐量从 5,846 涨到 20,220 req/s，P95 从 29ms 降到 12.7ms。同时 500 并发 5 万请求零错误。

**代码关联：**
- `internal/observability/metrics.go` -- Prometheus 指标定义
- `internal/observability/middleware.go` -- Gin metrics 中间件
- `internal/video/comment_repo.go:ListByVideoID` -- 缓存接入点
- `docs/optimization-report.md` -- 完整数据

**追问：**
- 为什么 P95 而不是 P50/P99？
- pprof 的 CPU profile 和 trace 区别？
- 怎么避免压测数据被缓存"虚高"？

---

### Q2: Prometheus 为什么用 Pull 模式？Push 模式有什么不行？

**参考答案：**

Pull 模式是 Prometheus 主动到服务的 `/metrics` 端点抓取，每 15s 一次。Push 模式是服务主动把指标推到 StatsD 之类的中心收集器。

Pull 的优势：

第一，**服务方不需要知道监控系统的地址**，所有配置集中在 Prometheus 端，部署解耦。

第二，**能发现死掉的目标**。Pull 模式下，目标挂了 scrape 就失败，up 指标变 0，可以直接告警。Push 模式下目标挂了就静默，监控系统不知道。

第三，**抓取频率由监控决定**，避免服务被告警系统反向压垮。Push 模式下高频指标可能压垮收集器。

但 Pull 也有限制：服务必须暴露 HTTP 端口，对短生命周期任务（比如 cron 任务）不友好，所以 Prometheus 还提供 PushGateway 作为补充。

**代码关联：**
- `cmd/main.go` -- 注册 `/metrics` 端点
- `configs/prometheus.yml` -- scrape 配置（targets, interval）

**追问：**
- 长尾任务（30 秒一次的 cron）怎么用 Prometheus？
- 多实例服务怎么避免抓取重复？
- /metrics 暴露在公网会有什么问题？

---

### Q3: Counter、Gauge、Histogram、Summary 怎么选？项目里都用在哪？

**参考答案：**

四种指标的核心区别是**值的语义**：

- **Counter**：单调递增，比如请求总数。不能减。看趋势用 `rate()`。
- **Gauge**：可增可减，比如当前连接数、队列长度。看瞬时值。
- **Histogram**：记录值的分布，比如请求延迟。客户端只暴露 buckets，分位数在服务端用 `histogram_quantile` 计算。
- **Summary**：服务端预聚合的分位数。优点是查询便宜，缺点是不能跨实例聚合，几乎不用。

项目里：

```go
// Counter: HTTP 请求数、MQ 消息数、熔断拒绝数
HTTPRequestsTotal               // CounterVec
MQMessagesPublished             // CounterVec
CircuitBreakerRejections        // Counter

// Histogram: 延迟分布
HTTPRequestDuration             // HistogramVec
RedisOperationDuration          // HistogramVec
```

我没用 Gauge，因为业务上暂时没有需要实时查看的"瞬时值"场景。不过有一个 trade-off：熔断器的**当前状态**其实适合用 Gauge（0=Closed, 1=Open, 2=HalfOpen），但我选择用 Counter 记录状态切换次数。好处是能看到历史趋势（每分钟切了几次 Open），缺点是无法直接查"现在是什么状态"。如果未来要做告警（"熔断器现在是 Open"），需要补一个 Gauge。

**桶选型**：HTTP 延迟用 1ms~5s 的桶（覆盖正常和慢请求），Redis 用 0.1ms~1s 的桶（Redis 大多数操作亚毫秒）。桶选错了 P95/P99 就失真。

**代码关联：**
- `internal/observability/metrics.go` -- 所有指标定义和桶配置

**追问：**
- 桶的数量越多越好吗？
- 怎么计算 Redis 缓存命中率？要不要加专门的 Counter？
- HTTPRequestDuration 的 path label 用 `c.FullPath()` 而不是 `c.Request.URL.Path`，为什么？

**项目 9 个 Prometheus 指标完整清单**：

| 指标名 | 类型 | 标签 | 采集方式 |
|--------|------|------|----------|
| `feedsystem_http_requests_total` | Counter | method, path, status_code | Gin middleware 自动 |
| `feedsystem_http_request_duration_seconds` | Histogram | method, path | Gin middleware 自动 |
| `feedsystem_redis_operations_total` | Counter | operation, status | cache.go/zset.go/set.go 手动 |
| `feedsystem_redis_operation_duration_seconds` | Histogram | operation, status | cache.go/zset.go/set.go 手动 |
| `feedsystem_mq_messages_published_total` | Counter | exchange, routing_key | PublishJSON 手动 |
| `feedsystem_mq_messages_consumed_total` | Counter | queue | Worker handleDelivery 手动 |
| `feedsystem_circuit_breaker_state_changes_total` | Counter | to_state | breaker.go OnStateChange |
| `feedsystem_circuit_breaker_rejections_total` | Counter | — | breaker.go Execute |
| `feedsystem_ratelimit_rejections_total` | Counter | limiter, prefix | ratelimit 中间件 |

---

### Q4: 你们项目 Redis 挂了会怎样？熔断器解决了什么问题？

**参考答案：**

我们项目最初的降级逻辑是 `if cache != nil`：Redis 客户端是 nil 就直接走 MySQL。但这只能处理"Redis 进程挂了，连不上"的场景。

更糟的场景是 Redis **半挂**：网络抖动每次响应 200ms，或者部分命令成功部分失败。简单判 nil 处理不了：

- 客户端不是 nil（连接还在）
- 每个请求都要等 200ms 超时
- 连接池被打满
- 最后请求堆积 → MySQL 也被压垮 → 雪崩

熔断器的价值就是识别"系统正在变差"，主动断流。我用了 sony/gobreaker v2，三态机：

- **Closed**：正常放行，统计连续失败
- **Open**：连续 5 次失败后切到 Open，直接拒绝（返回 ErrBreakerOpen），让上层走降级路径
- **HalfOpen**：10s 后切到 HalfOpen 放 1 个探测请求，成功就回 Closed，失败就回 Open

关键设计：**`redis.Nil`（缓存未命中）不计入失败**，因为它是正常的回源信号，不是 Redis 故障。

熔断器开启时上层走降级（直接查 MySQL），系统继续可用，只是没缓存加速。Redis 恢复后 10s 自动探测，无需人工介入。

具体降级策略：
- **ListLatest**：检测到 `ErrBreakerOpen` 时调用 `listLatestFromDB` 直查 MySQL
- **Pipeline 调用方**（fanoutworker、socialworker、feed/service、sliding_window）：通过 `IsBreakerOpen()` 预检查，跳过 Pipeline 执行走逐个写入或 fail-open
- **Worker**（outboxworker、fanoutworker）：收到 `ErrBreakerOpen` 时 sleep 1s 再 Nack-requeue，避免空转热循环

**代码关联：**
- `internal/middleware/redis/breaker.go:Breaker.Execute` -- 熔断包装
- `internal/middleware/redis/cache.go + zset.go + set.go` -- 所有 Redis 操作接入熔断
- `internal/middleware/redis/cache.go:IsBreakerOpen` -- Pipeline 调用方预检查
- `internal/feed/service.go:listLatestFromDB` -- ListLatest 熔断降级 MySQL
- `internal/worker/fanoutworker.go + outboxworker.go` -- ErrBreakerOpen 时 sleep 1s 退避
- `internal/observability/metrics.go:CircuitBreakerStateChanges` -- 监控

**追问：**
- 为什么 HalfOpen 失败要立即回 Open？多放几个请求验证不行吗？
- 5 次失败的阈值怎么定的？太敏感会怎样？
- 熔断器和限流器有什么区别？
- 雪崩场景下应该怎么联动多个熔断器？
- 熔断器开启后所有请求直接打 MySQL，MySQL 扛得住吗？怎么防止级联雪崩？
- `IsBreakerOpen()` 对 half_open 状态也返回 true，这意味着探测期 Pipeline 路径也会跳过 Redis，会不会过度降级？（是的，这是当前实现的 trade-off：Pipeline 无法使用 `Execute` 封装参与探测，宁可过度降级也不冒险）

---

### Q5: 固定窗口和滑动窗口限流有什么区别？为什么要做滑动窗口？

**参考答案：**

最简单的限流是固定窗口：用 Redis `INCR + PEXPIRE` 一条命令搞定。但有个经典缺陷叫**边界突刺**。

假设 1 分钟限流 100 次，攻击者可以在第 59 秒发 100 次，紧接着第 61 秒（新窗口）又发 100 次。**2 秒内 200 次**，是限流值的 2 倍。对登录这种低频接口可能没事，但对点赞这种 30 次/分钟的高频接口就危险了。

滑动窗口的解法是把时间维度也存进 Redis，用 ZSET：

```
member = 唯一标识（纳秒时间戳）
score = 时间戳

每次请求：
1. ZREMRANGEBYSCORE: 清理窗口外的过期请求
2. ZCARD: 统计窗口内请求数
3. < limit 则 ZADD 当前请求 + PEXPIRE
4. >= limit 则拒绝
```

整段逻辑封装在 Lua 脚本里原子执行，保证不会出现"读到旧计数后再写"的竞态。

ZSET 里始终只有"过去 window 时间内的请求"，无论何时切片都严格不超过 limit。我写了测试 `TestSlidingWindowNoBoundaryBurst`：限流 3 次 / 200ms，前 3 次 OK，到第 199ms（接近边界但未跨越）第 4 次必须被 429 拒绝。

**代价**：Lua 脚本内部执行 4 条 Redis 命令（ZREMRANGEBYSCORE + ZCARD + ZADD + PEXPIRE），但从客户端视角只有 **1 次网络往返**（1 次 EVAL 调用）。所以延迟只比固定窗口多 ~0.3ms（Lua 解释开销），不是 4 倍。内存占用 O(limit) vs 固定窗口的 O(1)。

项目保留两套，按业务选：登录注册用固定窗口（低频，简单），点赞评论用滑动窗口（高频，需要严格）。

**代码关联：**
- `internal/middleware/ratelimit/ratelimit.go:Limit` -- 固定窗口
- `internal/middleware/ratelimit/sliding_window.go:SlidingWindowLimit` -- 滑动窗口
- `internal/middleware/ratelimit/sliding_window_test.go:TestSlidingWindowNoBoundaryBurst` -- 边界验证

**追问：**
- ZSET member 为什么用纳秒时间戳不用毫秒？
- 滑动日志 vs 滑动计数器有什么区别？
- 还有什么限流算法？令牌桶、漏桶各自适合什么场景？

---

### Q6: 滑动窗口的 Lua 脚本为什么必须原子？拆开会怎样？

**参考答案：**

如果不用 Lua，4 步操作分开发：

```
client → ZREMRANGEBYSCORE
client → ZCARD（拿到 count）
判断 count < limit
client → ZADD
client → PEXPIRE
```

中间任何一个步骤别的请求都可能插进来，引发**竞态**：

- 请求 A 执行 ZCARD 得到 99（limit=100）
- 请求 B 同时执行 ZCARD 也得到 99
- A 判断通过，ZADD（变成 100）
- B 判断通过，ZADD（变成 101，超过 limit 但已经允许了）

这就是经典的 TOCTOU（Time-of-check to Time-of-use）。

Lua 脚本在 Redis 里是**单线程串行执行**，整段脚本对其他客户端原子可见。要么全做完，要么没开始，不会被穿插。

固定窗口用 `INCR` 也是同样的逻辑：INCR 单条命令本身原子，但如果拆成 `GET + 1 + SET` 就会有竞态。

**代码关联：**
- `internal/middleware/ratelimit/sliding_window.go:slidingWindowScript`
- `internal/middleware/redis/redis.go:incrementWithExpireScript` -- 固定窗口的 Lua

**追问：**
- Lua 脚本会阻塞 Redis 主线程，长脚本怎么办？
- EVAL 和 EVALSHA 的区别？
- 集群模式下 Lua 怎么用？（slot 限制）

---

### Q7: 你的 k-way merge 实现是 O(N log K)，能讲讲为什么不是 O(NK)？

**参考答案：**

最朴素的归并方式是"每次扫描所有 K 个流取最大"，时间复杂度 O(NK)：每个元素都要和所有流的当前头部比较。

我的实现用堆做归并，复杂度 O(N log K)：

1. **初始化**：每个流的第一个元素入堆，堆大小 K
2. **每次 pop**：取堆顶（K 个流当前头部的最大值），O(log K)
3. **从 pop 出的流取下一个元素入堆**，O(log K)
4. **总共 pop N 次**，所以是 N log K

关键是堆里**只存 K 个元素**（每个流当前的头部），不是所有元素一起入堆（那样会变 N log N）。

代码（`feed/service.go:mergeAndDedup`）：

```go
type streamCursor struct {
    items  []VideoWithTime  // 已排序的流
    pos    int               // 当前位置
}

type mergeItem struct {
    video  VideoWithTime
    cursor *streamCursor    // 指回流，方便取下一个
}

// 最大堆，按 createTime 降序
type mergeHeap []mergeItem

func mergeAndDedup(cursors []*streamCursor, limit int) []uint {
    h := &mergeHeap{}
    // 每个流的第一个元素入堆
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
        // 从同一个流取下一个入堆
        if next, ok := top.cursor.peek(); ok {
            top.cursor.advance()
            top.video = next
            heap.Push(h, top)
        }
    }
    return result
}
```

我的 benchmark 数据：

- 5 流 × 20 条，limit=10：~1.8μs，34 次内存分配
- 10 流 × 100 条带重复，limit=50：~7.4μs
- 20 流 × 10 条，limit=5：~2.2μs

实际场景里 inbox + 大V outbox 通常 5-10 个流，每个流几十条，N log K 远优于 N log N。

**代码关联：**
- `internal/feed/service.go:mergeAndDedup` -- 主体实现
- `internal/feed/service.go:streamCursor / mergeHeap` -- 数据结构
- `internal/feed/service_test.go:BenchmarkMergeAndDedup` -- 性能数据
- `internal/feed/service_test.go:TestMergeAndDedup_*` -- 9 个边界 case 测试

**追问：**
- 为什么要去重？哪些场景下会出现重复？
- 如果某个流中途出错怎么办？
- 怎么扩展支持降序+升序混合？
- 如果 limit=5 但关注了 20 个大V，堆初始化 push 20 次值得吗？（提示：K 通常 < 10，且配额分配阶段已跳过冷大V，实际入堆的流数量有限）

---

| 问题 | 一句话速答 |
|------|-----------|
| singleflight 和分布式锁区别？ | singleflight 单进程去重，分布式锁跨实例保护。本项目单机部署下两者效果一样，用两种技术是为了丰富技术栈 |
| 为什么解锁用 Lua？ | 保证 GET+比较+DEL 原子，防止释放别人的锁 |
| 限流 Lua 为什么必须原子？ | 防止 INCR 后 PEXPIRE 未执行导致 key 永不过期 |
| 三级缓存为什么需要 L1？ | 减少 Redis 网络往返，热点数据亚毫秒响应 |
| ZSET 在项目中的用途？ | 时间线分页 + 热度窗口计数 + 时间衰减加权合并热榜（decay=0.95） |
| Outbox 模式解决什么？ | 写 DB 和发 MQ 不能原子的问题 |
| 冷热分离的 watermark 是什么？ | ZSET 最老一条的 score，区分走 Redis 还是 MySQL |
| 冷数据为什么不回写 ZSET？ | 防止冷数据污染热点，ZSET 只保留最新 1000 条 |
| TOCTOU 怎么解决？ | 点赞不预检查依赖唯一索引，关注保留预检查+兜底 |
| MQ 不可用怎么办？ | 自动降级到同步 MySQL 事务，数据不丢 |
| 幂等消费怎么实现？ | MySQL 唯一索引 + catch 1062 静默忽略 |
| JWT 吊销怎么做？ | Redis 存 token，每次请求比对，不一致即吊销 |
| GREATEST 防什么？ | 防止点赞计数减成负数 |
| SoftJWTAuth 什么场景？ | 公共接口可选认证，有 token 解析身份，没有也放行 |
| 游标分页为什么比 offset 好？ | 始终 O(log N + limit)，不会因为偏移量大变慢 |
| 三字段游标为什么需要三个字段？ | 打破 popularity 和 create_time 的并列 |
| 固定窗口限流缺点？ | 窗口边界突发 2x 流量 |
| bcrypt 为什么比 SHA256 适合密码？ | 自带 salt + 计算慢，防暴力破解 |
| 优雅停机怎么实现？ | signal.NotifyContext + srv.Shutdown 等待请求完成 |
| Dockerfile 多阶段构建好处？ | 最终镜像只有二进制，体积小安全性好 |
| 推拉结合怎么实现？ | 普通用户推活跃粉丝 inbox，大V拉 outbox，读时公平提前终止+活跃度优先+k-way merge 归并 |
| 粉丝活跃度过滤？ | 登录写 user:active:{id} TTL 72h，fanout 时 Pipeline EXISTS 批量过滤，僵尸粉走拉路径兜底 |
| k-way merge 复杂度？ | O(N log K) 时间，O(K+limit) 空间，堆大小=流数量 K，streamCursor 迭代器+mergeHeap |
| 恒定 RTT 怎么做到？ | 并行读取 + Pipeline 批量探测+拉取 + 公平提前终止，RTT **次数**恒定约 3 次，不随大V数量增长 |
| 提前终止公平性？ | Pipeline 批量查所有大V最新时间戳取 max，非只采样 1 个 |
| 拉取配额分配？ | 按 newestAt 降序排序，跳过冷大V（newestAt <= inboxOldest），最活跃大V优先分配配额 |
| 收件箱概率裁剪？ | 1% 概率触发 ZREMRANGEBYRANK 裁剪到 500，参考 Stream-Framework，摊还裁剪开销 |
| 评论缓存迭代？ | 从整体缓存（50KB）到分页缓存（2KB），25 倍缩小，只失效第 1 页靠 TTL 兜底 |
| IsBreakerOpen half_open？ | 对 half_open 也返回 true，Pipeline 路径跳过 Redis，可能过度降级是 trade-off |
| follower_count 缓存风险？ | TTL=0 永不过期，event missed 会永久脏数据，生产应设 24h TTL |
| 熔断器三态？ | Closed（正常）→ Open（拒绝）→ HalfOpen（探测），连续 5 次失败触发，10s 后探测 |
| 为什么 redis.Nil 不计入熔断？ | 缓存未命中是正常回源信号，不是 Redis 故障 |
| 滑动窗口 vs 固定窗口？ | 滑动窗口无边界突刺，任意时刻窗口内严格不超 limit；代价是 O(limit) 内存 |
| 滑动窗口 member 为什么用纳秒？ | 毫秒级可能重复导致 ZADD 覆盖，计数偏少；纳秒也可能并发重复，实际用纳秒+原子计数器保证唯一 |
| Prometheus Pull vs Push？ | Pull 能发现死目标、抓取频率可控、服务方无需知道监控地址 |
| Histogram 桶选错会怎样？ | 所有请求落同一桶，P95/P99 失真，无法看出分布 |
| k-way merge 为什么 O(N log K)？ | 堆大小=K（流数量），每次 pop+push 是 O(log K)，共 N 次 |
| 可观测性闭环？ | Grafana 发现异常 → PromQL 定位 → pprof 确认 → 源码分析 → 优化 → hey 复测 → Grafana 验证 |

---

## 10. 深度追问与高频经典题（10 题）

> 以下问题是面试中高频出现的"深度追问"，通常在你回答完基础问题后面试官会继续追问。

### Q1: 你的系统怎么保证数据最终一致性？如果中间某个环节失败了呢？

**参考答案：**

我们系统是 AP 架构，追求最终一致性而非强一致性。保证机制有三层：

**第一层：Outbox 模式保证"写 DB 和发 MQ"的原子性。** 视频发布时在一个 MySQL 事务里同时写 video 表和 outbox 表。即使 MQ 暂时不可用，outbox 记录不会丢，轮询器会持续重试。

**第二层：At-least-once + 幂等消费保证"消息不丢不重"。** MQ 消息设为 Persistent + 手动 Ack，消费者用唯一索引 catch 1062 保证幂等。即使消息重复投递，业务结果也是正确的。

**第三层：缓存 TTL + 主动失效保证"读到的数据最终正确"。** 写操作后主动 DEL 缓存，即使 DEL 失败，TTL 到期后缓存自然淘汰，下次读取从 DB 获取最新数据。

**失败场景分析：**

- Outbox 轮询器投递成功但 DELETE 失败 → 下次重复投递 → 消费者幂等忽略
- Worker 处理消息时 crash → 消息未 Ack → RabbitMQ 重新投递 → 幂等消费
- Redis DEL 缓存失败 → TTL 到期后自然淘汰 → 最多延迟 TTL 时间看到旧数据
- MySQL 事务回滚 → outbox 记录也回滚 → 不会发出错误消息

**追问：**
- 最终一致性的"最终"是多久？怎么量化？
- 如果业务要求强一致性（如扣款），怎么改造？
- 分布式事务的 Saga 模式和 Outbox 模式有什么区别？

---

### Q2: Redis 集群模式下，你的 Lua 脚本还能用吗？

**参考答案：**

不能直接用。Redis Cluster 模式下，Lua 脚本涉及的所有 key 必须在同一个 slot（哈希槽）里，否则会报 `CROSSSLOT` 错误。

我们项目有三个 Lua 脚本：
1. **限流脚本**（固定窗口）：只操作 1 个 key（`ratelimit:{biz}:{subject}`），天然兼容 Cluster。
2. **解锁脚本**：只操作 1 个 key（`lock:{cacheKey}`），天然兼容。
3. **滑动窗口脚本**：只操作 1 个 key（ZSET），天然兼容。

所以我们的 Lua 脚本恰好都是单 key 操作，迁移到 Cluster 不需要改动。

但如果有多 key 的 Lua 脚本（比如 ZUNIONSTORE 合并 60 个窗口），需要用 **Hash Tag** 保证相关 key 落在同一个 slot：`hot:video:1m:{video}:202605061435`，花括号内的部分决定 slot。

**追问：**
- Hash Tag 会不会导致数据倾斜？
- Redis Cluster 的 slot 迁移过程中，Lua 脚本会怎样？
- 除了 Hash Tag，还有什么方案？（提示：应用层拆分）

---

### Q3: 如果让你设计一个支持"撤回点赞"的系统，怎么保证撤回和点赞不会并发冲突？

**参考答案：**

核心问题是：用户快速点赞再取消（或反过来），两条 MQ 消息可能乱序到达 Worker。

我们项目的解决方案是**事务内幂等 + 唯一索引**：

- 点赞：`LikeIgnoreDuplicateInTx` 尝试 INSERT，如果已存在（1062）则 created=false，不更新计数
- 取消点赞：`DeleteByVideoAndAccountInTx` 尝试 DELETE，如果不存在则 affected=0，不更新计数

关键是：**计数更新和记录操作在同一个事务内**。无论消息以什么顺序到达，最终状态都是正确的：

```
场景：用户点赞 → 取消点赞，但 MQ 消息乱序
Worker 先收到 unlike → DELETE 返回 affected=0（记录不存在）→ 不减计数
Worker 再收到 like → INSERT 成功 → +1 计数
最终状态：有点赞记录，计数 +1（错误！用户实际已取消）
```

等等，这里有个问题！如果消息乱序，最终状态确实可能不一致。我们项目的应对是：

1. **同一用户同一视频的消息有序性**：RabbitMQ 单队列内消息是 FIFO 的，同一个 Producer 发出的消息顺序不会乱
2. **降级路径是同步的**：API 进程内同步执行，天然有序
3. **即使极端情况乱序**：用户再次操作时会修正状态（最终一致）

**追问：**
- 如果用 Kafka，怎么保证同一用户的消息有序？（提示：partition key）
- 如果要做到强一致，需要什么？（提示：版本号/时间戳比较）
- 乐观锁 vs 悲观锁在这个场景下怎么选？

---

### Q4: 你的三级缓存在多实例部署下有什么一致性问题？

**参考答案：**

L1 本地缓存是进程内的，多实例之间不共享。这意味着：

**问题场景：**
1. 实例 A 更新了视频数据，DEL 了 Redis 缓存
2. 实例 B 的 L1 本地缓存还有旧数据（TTL 3-5 秒内）
3. 用户请求打到实例 B，读到旧数据

**我们的容忍度：** L1 TTL 只有 3-5 秒，最多延迟 5 秒看到更新。对于短视频 Feed 流来说，5 秒的不一致完全可以接受。

**如果要更强的一致性：**
1. **Redis Pub/Sub 广播失效**：更新时发布失效消息，所有实例订阅后清除本地缓存
2. **版本号比较**：L1 缓存带版本号，读取时和 Redis 版本比较
3. **缩短 TTL**：L1 TTL 降到 1 秒，代价是 Redis QPS 上升

我们选择"容忍 3-5 秒不一致"是因为：Feed 流本身就不是强一致场景，用户刷新一下就能看到最新数据。

**追问：**
- 如果是电商库存场景，L1 缓存还能用吗？
- Redis Pub/Sub 在 Cluster 模式下有什么限制？
- 本地缓存的内存上限怎么控制？go-cache 有 LRU 淘汰吗？

---

### Q5: 你的热榜 ZUNIONSTORE 合并 60 个 key，性能开销大吗？怎么优化？

**参考答案：**

ZUNIONSTORE 的时间复杂度是 O(N*K + M*log(M))，N 是输入 key 数量（60），K 是每个 key 的平均 member 数，M 是结果集大小。

假设每个分钟窗口有 100 个活跃视频，60 个窗口合并：
- 输入总量：60 × 100 = 6000 个 member
- 去重后结果集：假设 2000 个唯一视频
- 时间复杂度：O(6000 + 2000*log(2000)) ≈ O(6000 + 22000) ≈ O(28000)

在 Redis 单线程下，这个操作大约耗时 1-5ms，对于热榜查询来说可以接受。

**我们的优化：**
1. **结果缓存 2 分钟**：`Exists(dest)` 检查，命中直接用，不重复合并
2. **只在首次查询时合并**：同一分钟内的所有请求共享同一个合并结果
3. **ZSET 自动过期**：分钟窗口 2 小时过期，不会无限膨胀

**进一步优化方向：**
- 预计算：用定时任务每分钟提前合并，查询时直接读结果
- 分层合并：先合并最近 10 分钟（高频更新），再合并 10-60 分钟（低频更新）
- 近似算法：用 HyperLogLog 或 Top-K 近似代替精确排序

**追问：**
- 如果视频量从 100 涨到 10 万，ZUNIONSTORE 还能用吗？
- Redis 的 ZUNIONSTORE 是阻塞操作吗？会影响其他命令吗？
- 时间衰减的 decay=0.95 怎么调？调大调小分别什么效果？

---

### Q6: 你的 Worker 消费速度跟不上生产速度怎么办？

**参考答案：**

这是经典的**消费积压**问题。判断标准是 RabbitMQ 管理界面上队列深度持续增长。

**短期应对：**
1. **增加 Worker 实例**：Worker 是无状态的，直接加机器。多个 Worker 消费同一个队列，RabbitMQ 会 round-robin 分发。
2. **调大 prefetchCount**：从 50 调到 200，让每个 Worker 一次拿更多消息，减少网络往返。但不能太大，否则消息分配不均。

**中期优化：**
1. **批量处理**：Worker 攒一批消息后批量写 MySQL（如 100 条一次 INSERT），减少事务开销。
2. **并行消费**：Worker 内部用 goroutine pool 并行处理消息，但要注意同一 video_id 的消息不能并行（会有计数竞态）。

**长期方案：**
1. **换 Kafka**：Kafka 的 partition 模型天然支持水平扩展，吞吐量比 RabbitMQ 高一个数量级。
2. **削峰填谷**：MQ 本身就是缓冲区，允许短时积压，只要 Worker 的平均消费速度 >= 平均生产速度就行。

**追问：**
- 消息积压时，最老的消息可能已经过时了，怎么处理？
- 多个 Worker 消费同一队列，消息顺序还能保证吗？
- RabbitMQ 和 Kafka 在消费模型上的本质区别？

---

### Q7: 如果数据库连接池打满了，系统会怎样？你怎么排查？

**参考答案：**

**现象：** 请求延迟突增，P99 飙升到几百毫秒甚至超时。日志中出现 `connection pool exhausted` 或 GORM 的 `context deadline exceeded`。

**排查步骤：**
1. **看 Prometheus**：HTTP P95/P99 延迟突增，Redis 延迟正常 → 瓶颈在 MySQL
2. **看 MySQL**：`SHOW PROCESSLIST` 看活跃连接数，`SHOW STATUS LIKE 'Threads_connected'` 看总连接数
3. **看 pprof**：`go tool pprof http://localhost:6062/debug/pprof/goroutine` 看是否有大量 goroutine 阻塞在 `database/sql.(*DB).conn`
4. **看连接池配置**：`MaxOpenConns` 是否太小，`MaxIdleConns` 是否太小导致频繁建连

**我们的配置：**
```go
MaxOpenConns = 100    // 最大连接数
MaxIdleConns = 25     // 空闲连接数
ConnMaxLifetime = 5min // 连接最大存活
ConnMaxIdleTime = 3min // 空闲连接最大存活
```

**为什么 MaxOpenConns=100：** MySQL 默认 max_connections=151，留 51 给系统和其他客户端。

**追问：**
- MaxIdleConns 设太大会怎样？（占用 MySQL 连接资源）
- ConnMaxLifetime 为什么要设？（防止连接被 MySQL 端超时断开后客户端不知道）
- 如果有 3 个 API 实例，每个 MaxOpenConns=100，MySQL 需要支持多少连接？

---

### Q8: 你的系统有没有做过容量评估？怎么估算需要多少资源？

**参考答案：**

容量评估的核心公式：**资源需求 = 峰值 QPS × 单请求资源消耗 × 安全系数**

**以我们项目为例：**

假设目标：支撑 10 万 DAU，人均刷 50 条 Feed。

1. **读 QPS 估算**：
   - 日请求量 = 10万 × 50 = 500万
   - 峰值系数 = 3（集中在晚 8-10 点）
   - 峰值 QPS = 500万 / (2×3600) × 3 ≈ 2000 QPS

2. **写 QPS 估算**：
   - 点赞率 10%，评论率 2%
   - 点赞 QPS = 2000 × 10% = 200
   - 评论 QPS = 2000 × 2% = 40

3. **资源需求**：
   - API 实例：压测单实例 20000 QPS，2000 QPS 只需 1 台（安全系数 3 → 3 台）
   - Redis：单实例 10万 QPS，2000 QPS 绰绰有余
   - MySQL：读 2000 QPS（大部分被缓存拦截，实际打到 DB 约 400 QPS），单实例够用
   - RabbitMQ：写 240 QPS，单实例够用

**追问：**
- 安全系数一般取多少？为什么？
- 如果突然有个视频爆火，QPS 翻 10 倍怎么办？
- 怎么做弹性扩缩容？

---

### Q9: Go 的 GC 对你的系统有什么影响？怎么优化？

**参考答案：**

Go 的 GC 是并发标记-清除（concurrent mark-sweep），STW（Stop-The-World）时间通常在微秒到毫秒级。对于我们的系统：

**潜在影响：**
1. **L1 本地缓存**：go-cache 存大量对象，GC 扫描压力大。如果缓存了 10 万个视频详情，每次 GC 都要扫描这些对象。
2. **singleflight 结果共享**：大对象（如 `[]*video.Video`）被多个 goroutine 引用，GC 需要追踪引用关系。
3. **高并发下 goroutine 数量**：每个请求一个 goroutine，高并发时可能有几千个 goroutine，GC 需要扫描所有 goroutine 的栈。

**优化方向：**
1. **减少堆分配**：singleflight 结果用 `safeCopy` 深拷贝，避免共享大对象
2. **对象池**：频繁创建的小对象用 `sync.Pool` 复用
3. **GOGC 调优**：默认 GOGC=100（堆增长 100% 触发 GC），可以调大到 200 减少 GC 频率，代价是内存占用增加
4. **pprof 监控**：`/debug/pprof/heap` 看内存分配热点，`/debug/pprof/gc` 看 GC 暂停时间

**追问：**
- Go 1.19 引入的 GOMEMLIMIT 是什么？和 GOGC 的关系？
- 如果 GC 暂停时间超过 10ms，对 P99 延迟有什么影响？
- sync.Pool 在 GC 时会被清空吗？

---

### Q10: 你的项目有哪些单点故障？怎么消除？

**参考答案：**

当前架构的单点故障：

| 组件 | 单点风险 | 影响 | 消除方案 |
|------|---------|------|---------|
| MySQL | 主库挂了 | 全部写操作失败 | 主从复制 + 自动 failover（MHA/Orchestrator） |
| Redis | 单实例挂了 | 缓存失效，全部打 DB | Redis Sentinel 或 Cluster |
| RabbitMQ | 单节点挂了 | MQ 不可用，走降级路径 | 镜像队列 + 集群 |
| API 进程 | 单实例挂了 | 服务不可用 | 多实例 + 负载均衡 |
| Worker 进程 | 单实例挂了 | 消息积压 | 多实例消费同一队列 |

**当前的容错设计：**
- Redis 不可用 → 降级 MySQL（已实现）
- RabbitMQ 不可用 → 同步写 MySQL（已实现）
- Worker crash → 消息未 Ack，RabbitMQ 重新投递（已实现）

**未消除的单点：**
- MySQL 主库是真正的单点，挂了整个系统不可用
- Outbox 轮询器只有一个实例在跑，挂了时间线不更新

**追问：**
- MySQL 主从切换时，正在进行的事务会怎样？
- 如果要做 MySQL 双主，有什么风险？
- 怎么做到"零停机"部署？

---

## 11. 可观测性实战：端到端瓶颈定位案例

### Q1: 用 Prometheus + Grafana + pprof 完整走一遍性能优化闭环

**参考答案：**

这是一个从"发现问题"到"验证修复"的完整链路，以 `comment/listAll` 为例：

**第一步：Grafana 发现异常**

接入 Prometheus + Grafana 后，看 "HTTP P95 by path" 面板。`comment/listAll` 的 P95 持续 >25ms，是 `feed/listLatest`（1.1ms）的 22 倍。这就是异常信号。

**第二步：PromQL 定位慢在哪**

```promql
# 对比各接口 P95
histogram_quantile(0.95, rate(feedsystem_http_request_duration_seconds_bucket{path="/comment/listAll"}[5m]))
histogram_quantile(0.95, rate(feedsystem_http_request_duration_seconds_bucket{path="/feed/listLatest"}[5m]))

# 看 Redis 操作延迟是否正常
histogram_quantile(0.95, rate(feedsystem_redis_operation_duration_seconds_bucket[5m]))
```

Redis P95 正常（~8ms），说明瓶颈不在 Redis，在 MySQL。

**第三步：pprof 确认**

```bash
go tool pprof http://localhost:6062/debug/pprof/profile?seconds=30
# top 命令看到 70% 时间在 GORM 的 MySQL 调用
```

**第四步：MySQL 确认**

```sql
SHOW PROCESSLIST;  -- 看到连接数打到默认 2 的上限
SHOW STATUS LIKE 'Threads_connected';
```

**第五步：源码分析定位两个瓶颈**

1. `comment/listAll` 每次打 2 次 MySQL（IsExist + GetAllComments），无缓存
2. MySQL 连接池用默认配置（MaxOpenConns=0, MaxIdleConns=2）

**第六步：优化**

- 加 Redis 分页缓存（key 带 page+size，TTL 30s，写时 DEL）
- 调连接池 MaxOpenConns=100, MaxIdleConns=25

**第七步：复测验证**

```bash
hey -n 20000 -c 200 -m POST -H "Content-Type: application/json" \
    -d '{"video_id":1,"page":1}' http://localhost:8080/comment/listAll
```

结果：吞吐量 5,846 → 20,220 req/s（+246%），P95 29.1ms → 12.7ms（-56%）。

同时在 Grafana 上验证：
```promql
# 确认 Redis 缓存命中
rate(feedsystem_redis_operations_total{operation="get",status="success"}[1m])
# 确认无 500 错误
rate(feedsystem_http_requests_total{status_code="500"}[1m])
```

**追问：**
- 如果 P95 高但 P50 正常，说明什么？
- pprof 的 CPU profile 和 heap profile 分别看什么问题？
- 怎么区分是应用层慢还是数据库层慢？
