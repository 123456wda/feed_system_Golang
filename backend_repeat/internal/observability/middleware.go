package observability

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

// 自动采集HTTP QPS和延迟
func MetricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 排除 /metrics 端点，避免 Prometheus scrape 自身产生监控噪声
		if c.FullPath() == "/metrics" {
			c.Next()
			return
		}

		// 记录请求开始时间
		start := time.Now()
		// 让请求继续向下走
		c.Next()

		// 请求结束后，采集指标
		duration := time.Since(start).Seconds()       // 耗时（秒）
		path := c.FullPath()                          // 路由模板，如 /video/getDetail
		method := c.Request.Method                    // HTTP 方法
		statusCode := strconv.Itoa(c.Writer.Status()) // 状态码转字符串

		// 记录 QPS（Counter +1）
		HTTPRequestsTotal.With(prometheus.Labels{
			"method":      method,
			"path":        path,
			"status_code": statusCode,
		}).Inc()

		// 记录延迟（Histogram 写入一次观测值）
		HTTPRequestDuration.With(prometheus.Labels{
			"method": method,
			"path":   path,
		}).Observe(duration)
	}
}
