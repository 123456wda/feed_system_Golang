package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestNewPprofMux 验证 pprof 路由 mux 能正确响应请求。
// 访问 /debug/pprof/ 应返回 200，说明所有 pprof handler 注册成功。
func TestNewPprofMux(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rr := httptest.NewRecorder()

	NewPprofMux().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status code 200, got %d", rr.Code)
	}
}

// TestNewPprofServerWithDisabled 验证 disabled 路径不会启动网络监听。
// 返回的结构体非空（便于统一调 Close），但内部 Server 为 nil（未监听端口）。
func TestNewPprofServerWithDisabled(t *testing.T) {
	t.Parallel()

	pprofServer, err := NewPprofServer("api", false, "localhost:6060")
	if err != nil {
		t.Fatalf("Failed to create pprof server: %v", err)
	}
	if pprofServer == nil {
		t.Fatalf("Expected non-nil pprof server struct when disabled")
	}
	if pprofServer.Server != nil {
		t.Fatalf("Expected nil inner Server when disabled, got non-nil")
	}
}

// TestPprofServerCloseWithDisabledServer 验证 Close() 对 disabled server 安全。
// 实际场景：服务 shutdown 时仍会调 Close()，如果没做 nil 保护就会 panic。
func TestPprofServerCloseWithDisabledServer(t *testing.T) {
	t.Parallel()

	pprofServer, err := NewPprofServer("api", false, "localhost:6060")
	if err != nil {
		t.Fatalf("Failed to create pprof server: %v", err)
	}
	if err := pprofServer.Close(); err != nil {
		t.Fatalf("Expected no error when closing disabled pprof server, got: %v", err)
	}
}
