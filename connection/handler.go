package connection

import (
	"context"
	"sync"

	"github.com/jursonmo/simple-message/protocol"
)

type Handler interface {
	Handle(request IRequest) error
}

type HandlerManager struct {
	readWriteCloser Conn
	conn            *Connection
	msgChan         <-chan *MessageBody
	handleMsg       func(conn *Connection, message *protocol.Message)
	decoder         *protocol.Decoder
	ctx             context.Context
	cancel          context.CancelFunc
	//wg              sync.WaitGroup
	err     error
	errOnce sync.Once
	done    chan struct{}
}

func NewHandlerManager(
	readWriteCloser Conn,
	handleMsg func(conn *Connection, message *protocol.Message),
	maxDataLen uint32,
	connectedBegin ConnectedBegin,
	data any,
) *HandlerManager {
	h := &HandlerManager{
		readWriteCloser: readWriteCloser,
		handleMsg:       handleMsg,
		decoder:         protocol.NewDecoder(maxDataLen),
		done:            make(chan struct{}),
	}
	h.conn, h.msgChan = NewConnection(data)
	h.ctx, h.cancel = context.WithCancel(context.Background())

	go func() {
		defer close(h.done)
		wg := &sync.WaitGroup{}
		defer wg.Wait()
		wg.Add(3)

		go func() {
			defer wg.Done()
			connectedBegin(h.ctx, h.conn)
		}()

		go func() {
			defer wg.Done()
			defer h.stop()
			h.read()
		}()

		go func() {
			defer wg.Done()
			defer h.stop()
			h.send()
		}()
	}()

	return h
}

func (h *HandlerManager) GetConnection() *Connection {
	return h.conn
}

func (h *HandlerManager) Stop() <-chan struct{} {
	h.stop()
	return h.done
}

func (h *HandlerManager) Ctx() context.Context {
	return h.ctx
}

func (h *HandlerManager) Err() error {
	<-h.done
	return h.err
}

func (h *HandlerManager) stop() {
	h.merr(ErrIsClose)
	h.conn.Close()
	h.readWriteCloser.Close()
	h.cancel()
}

func (h *HandlerManager) merr(err error) {
	h.errOnce.Do(func() {
		h.err = err
	})
}

func (h *HandlerManager) read() {
	for {
		if message, err := h.decoder.Unmarshal(h.readWriteCloser); err != nil {
			h.merr(err)
			return
		} else {
			h.handleMsg(h.conn, message)
		}
	}
}

func (h *HandlerManager) send() {
	var err error
	for {
		select {
		case <-h.conn.Ctx().Done():
			return
		case m := <-h.msgChan:
			m.AckMessage(func() error {
				message := m.GetMessage()
				err = h.decoder.Marshal(h.readWriteCloser, message.MsgID, message.Data)
				return err
			})

			if err != nil {
				h.merr(err)
				return
			}

		case <-h.ctx.Done():
			return
		}
	}
}
