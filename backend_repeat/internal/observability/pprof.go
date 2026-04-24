package observability

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

type PprofServer struct {
	Name            string // 这个区分是对worker的pprof还是对api的pprof
	Server          *http.Server
	shutdownTimeout time.Duration
}

func NewPprofMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)          // 这个表示pprof首页
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline) // 这个表示启动命令参数
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile) // 这个表示CPU消耗情况
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)   // 符号信息
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)     // execution trace
	return mux
}

func NewPprofServer(name string, enabled bool, addr string) (*PprofServer, error) {
	pprofServer := &PprofServer{
		Name:            name,
		shutdownTimeout: 3 * time.Second,
	}
	if !enabled || addr == "" {
		return pprofServer, nil
	}
	// 开始初始化ppro对应服务
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("Failed to start pprof: %s at addr: %s : %w", name, addr, err)
	}
	pprofServer.Server = &http.Server{
		Addr:        addr,
		Handler:     NewPprofMux(),
		ReadTimeout: 5 * time.Second,
	}

	// 创建一个协程启动服务器的监听
	go func() {
		log.Printf("%s pprof server listening on %s", name, addr)
		err := pprofServer.Server.Serve(listener)
		// http.ErrServerClosed表示服务正常关闭
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("%s pprof server error: %v", name, err)
		}
	}()

	return pprofServer, nil
}

func Shutdown(s *PprofServer, ctx context.Context) error {
	if s == nil || s.Server == nil {
		return nil
	}
	return s.Server.Shutdown(ctx)
}

func (s *PprofServer) Close() error {
	if s == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()
	err := Shutdown(s, shutdownCtx)
	if err != nil {
		log.Printf("%s pprof server shutdown error: %v", s.Name, err)
		return err
	}
	return nil
}
