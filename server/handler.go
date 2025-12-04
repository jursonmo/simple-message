package server

import (
	"context"
	"sync"

	"github.com/jursonmo/simple-message/connection"
	"github.com/jursonmo/simple-message/protocol"
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

		count := m.connCount.Add(1)
		if m.maxConnCount > 0 && count > m.maxConnCount {
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
		m.handleMsg,
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

func (m *Server) handleMsg(conn *connection.Connection, msg *protocol.Message) {
	m.handlerMu.RLock()
	handler, ok := m.handler[msg.MsgID]
	m.handlerMu.RUnlock()
	if !ok {
		m.stats.IncUnknownMsg()
		return
	}
	req := connection.NewRequest(
		conn,
		msg.MsgID,
		msg.Data,
	)

	if handler.Handle(req) == nil {
		m.stats.AddSuccessBytes(msg.MsgID, uint64(len(msg.Data)))
	} else {
		m.stats.AddFailedBytes(msg.MsgID, uint64(len(msg.Data)))
	}
}
