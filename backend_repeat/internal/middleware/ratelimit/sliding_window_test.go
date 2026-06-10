package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func newTestRouter(t *testing.T, mr *miniredis.Miniredis, max int64, window time.Duration) *gin.Engine {
	t.Helper()
	cache := rediscache.NewFromAddr(mr.Addr())
	r := gin.New()
	r.Use(SlidingWindowLimit(cache, "test", max, window, KeyByIP))
	r.POST("/ping", func(c *gin.Context) { c.String(200, "pong") })
	return r
}

func sendRequest(r *gin.Engine, ip string) int {
	req := httptest.NewRequest(http.MethodPost, "/ping", nil)
	req.RemoteAddr = ip + ":1234"
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w.Code
}

func TestSlidingWindowAllowsUnderLimit(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	r := newTestRouter(t, mr, 5, time.Second)

	for i := 0; i < 5; i++ {
		if code := sendRequest(r, "1.2.3.4"); code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i+1, code)
		}
	}
}

func TestSlidingWindowRejectsOverLimit(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	r := newTestRouter(t, mr, 3, time.Second)

	for i := 0; i < 3; i++ {
		if code := sendRequest(r, "1.2.3.4"); code != http.StatusOK {
			t.Fatalf("expected 200 for request %d, got %d", i+1, code)
		}
	}
	if code := sendRequest(r, "1.2.3.4"); code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", code)
	}
}

// TestSlidingWindowExpiry 验证旧请求随时间推移被移出窗口，配额恢复。
// 使用真实 time.Sleep 确保 ZREMRANGEBYSCORE 逻辑被正确触发。
func TestSlidingWindowExpiry(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	r := newTestRouter(t, mr, 2, 150*time.Millisecond)

	if sendRequest(r, "1.2.3.4") != http.StatusOK {
		t.Fatal("first request should pass")
	}
	if sendRequest(r, "1.2.3.4") != http.StatusOK {
		t.Fatal("second request should pass")
	}
	if sendRequest(r, "1.2.3.4") != http.StatusTooManyRequests {
		t.Fatal("third request should be rejected")
	}

	// 等待窗口过期，ZREMRANGEBYSCORE 会在下次请求时清理旧条目
	time.Sleep(160 * time.Millisecond)

	if sendRequest(r, "1.2.3.4") != http.StatusOK {
		t.Fatal("after window expiry, request should pass (ZREMRANGEBYSCORE cleans old entries)")
	}
}

func TestSlidingWindowSeparateKeys(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	r := newTestRouter(t, mr, 2, time.Second)

	sendRequest(r, "1.1.1.1")
	sendRequest(r, "1.1.1.1")
	if sendRequest(r, "1.1.1.1") != http.StatusTooManyRequests {
		t.Fatal("IP1 should be rejected after 2 requests")
	}
	if sendRequest(r, "2.2.2.2") != http.StatusOK {
		t.Fatal("IP2 should not be affected by IP1 rate limit")
	}
}

// TestSlidingWindowNoBoundaryBurst 验证滑动窗口无边界突刺。
// 在窗口内发满请求后，即使接近窗口边界，新请求仍被拒绝。
// 使用真实 time.Sleep 确保 ZREMRANGEBYSCORE 被正确执行。
func TestSlidingWindowNoBoundaryBurst(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer mr.Close()

	r := newTestRouter(t, mr, 3, 300*time.Millisecond)

	// t=0: 发 3 次成功
	for i := 0; i < 3; i++ {
		if sendRequest(r, "9.9.9.9") != http.StatusOK {
			t.Fatalf("initial req %d should pass", i+1)
		}
	}

	// 等待 150ms（窗口的一半），旧请求仍在窗口内
	time.Sleep(150 * time.Millisecond)

	// 第 4 次必须被拒绝（滑动窗口内仍有 3 条记录）
	if sendRequest(r, "9.9.9.9") != http.StatusTooManyRequests {
		t.Fatal("at mid-window, 4th request must be rejected (no boundary burst)")
	}

	// 再等 160ms（总共 310ms > 300ms 窗口），旧请求被清理
	time.Sleep(160 * time.Millisecond)

	// 配额恢复
	if sendRequest(r, "9.9.9.9") != http.StatusOK {
		t.Fatal("after full window elapsed, request should pass")
	}
}
