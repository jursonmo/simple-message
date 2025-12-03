package server

import (
	"context"
	"sync"

	"github.com/jursonmo/simple-message/connection"
)

func (m *Server) accept(ctx context.Context) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	defer m.cancel()
	for m.isRun.Load() {
		// 接受客户端的连接
		conn, data, err := m.listener.Accept()
		if err != nil {
			return
		}

		if m.connCount.Add(1) > m.maxConnCount {
			m.connCount.Add(-1)
			conn.Close()
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer conn.Close()
			defer m.connCount.Add(-1)
			m.handlerTcpConn(ctx, conn, data)
		}()

	}
}

func (m *Server) handlerTcpConn(ctx context.Context, conn connection.Conn, data any) {
	handlerManager := connection.NewHandlerManager(
		conn,
		m.handler,
		m.maxDataLen,
		m.action.ConnectedBegin,
		data,
	)
	defer func() {
		<-handlerManager.Stop()
		m.action.ConnErr(ctx, handlerManager.GetConnection(), handlerManager.Err())
	}()

	select {
	case <-ctx.Done():
		return
	case <-handlerManager.Ctx().Done():
		return
	}
}
