package server

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"

	"github.com/jursonmo/simple-message/connection"
	"github.com/jursonmo/simple-message/protocol"
	"github.com/jursonmo/simple-message/stats"
)

type Server struct {
	listener     Listener
	isRun        atomic.Bool
	ctx          context.Context
	cancel       context.CancelFunc
	startOnce    sync.Once
	statusMu     sync.Mutex
	action       Action
	handlerMu    sync.RWMutex
	handler      map[uint32]connection.Handler
	stats        *stats.HandlerStats
	maxDataLen   uint32
	maxConnCount int32
	connCount    atomic.Int32
	done         chan struct{}
}

type ServerOption func(*Server)

func WithMaxDataLen(maxDataLen uint32) ServerOption {
	return func(s *Server) {
		s.maxDataLen = maxDataLen
	}
}
func WithMaxConnCount(maxConnCount int32) ServerOption {
	return func(s *Server) {
		s.maxConnCount = maxConnCount
	}
}
func WithHandlers(handler map[uint32]connection.Handler) ServerOption {
	return func(s *Server) {
		s.handler = maps.Clone(handler)
	}
}

func NewServer(
	listener Listener,
	action Action,
	opts ...ServerOption,
) *Server {
	m := &Server{
		listener:   listener,
		action:     action,
		handler:    make(map[uint32]connection.Handler),
		maxDataLen: protocol.MaxDataLen,
		stats:      stats.NewHandlerStats(),
	}

	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Start 启动代理服务的各个组件
func (m *Server) Start(ctx context.Context, acceptAmount int) <-chan struct{} {
	m.startOnce.Do(func() {
		m.ctx, m.cancel = context.WithCancel(ctx)
		m.done = make(chan struct{})

		m.statusMu.Lock()
		defer m.statusMu.Unlock()
		m.setRunning(true)

		go func() {
			defer m.stop()
			wg := &sync.WaitGroup{}
			defer wg.Wait()
			wg.Add(acceptAmount)
			for i := 0; i < acceptAmount; i++ {
				go func() {
					defer wg.Done()
					m.accept(m.ctx)
				}()
			}
		}()
	})
	return m.done
}

// 两种方式可以停止server, 1. Stop 停止所有服务组件, 2. cancel  server's context,
// 所以为了一致性，只由cancel context 来停止server，Stop() 其实就是封装调用 cancel 方法而已。
func (m *Server) Stop() <-chan struct{} {
	if m.cancel == nil {
		return nil
	}
	m.cancel()
	m.listener.Close() //make m.listener.Accept() return error --> accept()  return --> wg.Wait() --> stop() --> close(m.done)
	return m.done
}

func (m *Server) stop() {
	m.statusMu.Lock()
	if !m.IsRunning() {
		m.statusMu.Unlock()
		return
	}
	m.setRunning(false)
	m.statusMu.Unlock()

	close(m.done)
}

func (m *Server) setRunning(isRun bool) {
	m.isRun.Store(isRun)
}
func (m *Server) IsRunning() bool {
	return m.isRun.Load()
}
