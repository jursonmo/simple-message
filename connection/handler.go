package connection

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jursonmo/simple-message/pkg/taskgo"
	"github.com/jursonmo/simple-message/protocol"
)

var useTaskgo bool = false

type Handler interface {
	Handle(request IRequest) error
}

type HandlerManager struct {
	sync.Once
	readWriteCloser Conn
	conn            *Connection
	handleMsg       func(conn *Connection, message *protocol.Message)
	decoder         *protocol.Decoder
	ctx             context.Context
	cancel          context.CancelFunc
	//wg              sync.WaitGroup
	err     error
	errOnce sync.Once
	done    chan struct{}
	// 任务管理器取消任务并返回后，会关闭done通道，表示handlerManager彻底停止。
	// 如果返回错误，可以看到是哪个任务没有正常退出。方便排查问题。
	taskmgr *taskgo.TaskGo
}

func NewHandlerManager(
	ctx context.Context,
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
	h.conn = NewConnection(readWriteCloser, data)
	h.ctx, h.cancel = context.WithCancel(ctx)

	if useTaskgo {
		h.taskmgr = taskgo.NewTaskGo(h.ctx)
		// 启动任务管理器
		h.taskmgr.Go("call begin", func(ctx context.Context) error {
			connectedBegin(h.ctx, h.conn)
			log.Printf("HandlerManager connected begin cb over\n")
			return nil
		})
		h.taskmgr.Go("read", func(ctx context.Context) error {
			defer h.stop()
			return h.read()
		})
		h.taskmgr.Go("send", func(ctx context.Context) error {
			defer h.stop()
			return h.send()
		})
		log.Printf("HandlerManager start ok\n")
		return h
	}

	// 启动goroutine任务,退出就关闭done通道。taskgo是在taskmgr结束后关闭done通道。
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

func (h *HandlerManager) MustStopWithTimeout(timeout time.Duration) {
	if useTaskgo {
		h.stopTaskWithTimeout(timeout, true)
		return
	}
	h.stopWithTimeout(timeout, true)
}

func (h *HandlerManager) StopWithTimeout(timeout time.Duration) {
	if useTaskgo {
		h.stopTaskWithTimeout(timeout, false)
		return
	}
	h.stopWithTimeout(timeout, false)
}

func (h *HandlerManager) stopWithTimeout(timeout time.Duration, panicOnTimeout bool) {
	select {
	case <-h.Stop():
		return
	case <-time.After(timeout):
		if panicOnTimeout {
			panic("HandlerManager stop timeout")
		}
		log.Printf("HandlerManager stop timeout: %v\n", timeout)
	}
}

func (h *HandlerManager) stopTaskWithTimeout(d time.Duration, panicOnTimeout bool) {
	err := h.taskmgr.StopAndWait(d)
	if err != nil {
		log.Printf("HandlerManager stop task timeout: %v\n", err)
		if panicOnTimeout {
			panic(err)
		}
	}
	close(h.done)
	log.Printf("HandlerManager stop() task over\n")
}

func (h *HandlerManager) Ctx() context.Context {
	return h.ctx
}

func (h *HandlerManager) Err() error {
	//<-h.done
	return h.err
}

func (h *HandlerManager) stop() {
	// 只执行一次
	h.Once.Do(func() {
		log.Printf("HandlerManager stop() \n")
		h.merr(ErrIsClose)
		h.conn.Close()
		h.readWriteCloser.Close()
		h.cancel()
		log.Printf("HandlerManager stop() over\n")
	})
}

func (h *HandlerManager) merr(err error) {
	h.errOnce.Do(func() {
		h.err = err
	})
}

// 主动退出时，read 任务的退出是靠close(readWriteCloser)触发的。
// 但是我发现不是每次close(readWriteCloser)都会触发read()退出。taskmgr可以看有时read()有时不会退出。
func (h *HandlerManager) read() error {
	var err error
	var message *protocol.Message
	defer func() {
		log.Printf("HandlerManager read() quit, err:%v\n", err)
	}()

	for {
		if message, err = h.decoder.Unmarshal(h.readWriteCloser); err != nil {
			h.merr(err)
			return err
		} else {
			h.handleMsg(h.conn, message)
		}
	}
}

func (h *HandlerManager) send() error {
	var err error
	defer func() {
		log.Printf("HandlerManager send() quit, err:%v\n", err)
	}()

	for {
		select {
		case <-h.conn.Ctx().Done():
			return h.conn.Ctx().Err()
		case m := <-h.conn.msgChan:
			m.AckMessage(func() error {
				message := m.GetMessage()
				err = h.decoder.Marshal(h.readWriteCloser, message.MsgID, message.Data)
				return err
			})

			if err != nil {
				h.merr(err)
				return err
			}

		case <-h.ctx.Done():
			return h.ctx.Err()
		}
	}
}
