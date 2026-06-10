package observability

import "github.com/prometheus/client_golang/prometheus"

//----HTTP层指标-----------

// HTTPRequestsTotal 统计每个接口的请求总数，按 method/path/status_code 分组
var HTTPRequestsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "feedsystem", // 项目名前缀，避免和其他指标冲突
		Name:      "http_requests_total",
		Help:      "Total number of HTTP requests",
	},
	[]string{"method", "path", "status_code"},
)

// HTTPRequestDuration 统计每个接口的请求延迟分布（直方图）
var HTTPRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "feedsystem",
		Name:      "http_request_duration_seconds",
		Help:      "HTTP request duration in seconds",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	},
	[]string{"method", "path"},
)

// ===== RabbitMQ 层指标 =====

// MQMessagesPublished 已发布的 MQ 消息数
var MQMessagesPublished = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "feedsystem",
		Name:      "mq_messages_published_total",
		Help:      "Total number of MQ messages published",
	},
	[]string{"exchange", "routing_key"},
)

// MQMessagesConsumed 已消费的 MQ 消息数
var MQMessagesConsumed = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "feedsystem",
		Name:      "mq_messages_consumed_total",
		Help:      "Total number of MQ messages consumed",
	},
	[]string{"queue"},
)

// ===== Redis 层指标 =====

// RedisOperationsTotal Redis 操作计数（status: success / error / miss）
var RedisOperationsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "feedsystem",
		Name:      "redis_operations_total",
		Help:      "Total number of Redis operations",
	},
	[]string{"operation", "status"},
)

// RedisOperationDuration Redis 操作延迟（status: success / error / miss）
var RedisOperationDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "feedsystem",
		Name:      "redis_operation_duration_seconds",
		Help:      "Redis operation duration in seconds",
		Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	},
	[]string{"operation", "status"},
)

// ===== 系统韧性指标（熔断器 + 限流） =====

// CircuitBreakerStateChanges 熔断器状态切换计数（标签：to_state = closed/open/half_open）。
// 通过观察该计数器的变化趋势可以判断 Redis 抖动频率与熔断恢复速度。
var CircuitBreakerStateChanges = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "feedsystem",
		Name:      "circuit_breaker_state_changes_total",
		Help:      "Total number of circuit breaker state transitions",
	},
	[]string{"to_state"},
)

// CircuitBreakerRejections 熔断器在 Open 状态拒绝的请求数。
// 该值非零意味着系统正在主动拒绝部分流量保护后端。
var CircuitBreakerRejections = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "feedsystem",
		Name:      "circuit_breaker_rejections_total",
		Help:      "Total number of requests rejected by circuit breaker",
	},
)

// RateLimitRejections 限流拒绝计数（标签：limiter = fixed/sliding，prefix = 业务前缀）。
var RateLimitRejections = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "feedsystem",
		Name:      "ratelimit_rejections_total",
		Help:      "Total number of requests rejected by rate limiter",
	},
	[]string{"limiter", "prefix"},
)

// 注册所有指标，import 时自动执行
func init() {
	prometheus.MustRegister(HTTPRequestsTotal)
	prometheus.MustRegister(HTTPRequestDuration)
	prometheus.MustRegister(RedisOperationsTotal)
	prometheus.MustRegister(RedisOperationDuration)
	prometheus.MustRegister(MQMessagesPublished)
	prometheus.MustRegister(MQMessagesConsumed)
	prometheus.MustRegister(CircuitBreakerStateChanges)
	prometheus.MustRegister(CircuitBreakerRejections)
	prometheus.MustRegister(RateLimitRejections)
}
