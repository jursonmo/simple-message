package server

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/jursonmo/simple-message/connection"
)

type Server struct {
	listener     Listener
	isRun        atomic.Bool
	ctx          context.Context
	cancel       context.CancelFunc
	startOnce    sync.Once
	statusMu     sync.Mutex
	action       Action
	handler      map[uint32]connection.Handler
	maxDataLen   uint32
	maxConnCount int32
	connCount    atomic.Int32
	done         chan struct{}
}

func NewServer(
	listener Listener,
	handler map[uint32]connection.Handler,
	maxDataLen uint32,
	maxConnCount int32,
	action Action,
) *Server {
	m := &Server{
		listener:     listener,
		action:       action,
		handler:      handler,
		maxDataLen:   maxDataLen,
		maxConnCount: maxConnCount,
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.done = make(chan struct{})
	return m
}

// Start 启动代理服务的各个组件
func (m *Server) Start(acceptAmount int) <-chan struct{} {
	m.startOnce.Do(func() {
		m.statusMu.Lock()
		defer m.statusMu.Unlock()
		m.isRun.Store(true)
		go func() {
			defer close(m.done)
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

// Stop 停止所有服务组件
func (m *Server) Stop() <-chan struct{} {
	m.statusMu.Lock()
	if !m.isRun.Load() {
		m.statusMu.Unlock()
		return nil
	}
	m.isRun.Store(false)
	m.statusMu.Unlock()

	m.listener.Close()
	m.cancel()
	return m.done
}
