package client

import (
	"context"
	"errors"
	"maps"
	"sync"
	"sync/atomic"

	"github.com/jursonmo/simple-message/connection"
	"github.com/jursonmo/simple-message/protocol"
	"github.com/jursonmo/simple-message/stats"
)

var (
	ErrIsClose = errors.New("已关闭")
	ErrConn    = errors.New("连接失败")
)

type Client struct {
	sync.RWMutex
	handler     map[uint32]connection.Handler
	ctx         context.Context
	cancel      context.CancelFunc
	maxDataLen  uint32
	action      Action
	done        chan struct{}
	connPointer atomic.Pointer[connection.Connection]

	stats *stats.HandlerStats
}

type ClientOption func(*Client)

func WithMaxDataLen(maxDataLen uint32) ClientOption {
	return func(c *Client) {
		c.maxDataLen = maxDataLen
	}
}
func WithHandlers(handler map[uint32]connection.Handler) ClientOption {
	return func(c *Client) {
		c.handler = maps.Clone(handler)
	}
}

func NewClient(
	action Action,
	opts ...ClientOption,
) *Client {
	c := &Client{
		action:     action,
		maxDataLen: protocol.MaxDataLen,
		handler:    make(map[uint32]connection.Handler),
		stats:      stats.NewHandlerStats(),
	}

	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *Client) AddHandler(MsgID uint32, handler connection.Handler) {
	c.Lock()
	defer c.Unlock()
	c.handler[MsgID] = handler
}

func (c *Client) RemoveHandler(MsgID uint32) {
	c.Lock()
	defer c.Unlock()
	delete(c.handler, MsgID)
}

func (c *Client) handleMsg(conn *connection.Connection, msg *protocol.Message) {
	c.RLock()
	handler, ok := c.handler[msg.MsgID]
	c.RUnlock()
	if !ok {
		c.stats.IncUnknownMsg()
		return
	}
	req := connection.NewRequest(
		conn,
		msg.MsgID,
		msg.Data,
	)
	if handler.Handle(req) == nil {
		c.stats.AddSuccessBytes(msg.MsgID, uint64(len(msg.Data)))
	} else {
		c.stats.AddFailedBytes(msg.MsgID, uint64(len(msg.Data)))
	}
}

func (c *Client) Start(ctx context.Context) {
	c.ctx, c.cancel = context.WithCancel(ctx)

	c.done = make(chan struct{})
	go func() {
		defer close(c.done)
		c.start()
		c.cancel()
	}()
}

func (c *Client) Stop() <-chan struct{} {
	c.cancel()
	return c.done
}

func (c *Client) start() {
	for c.action != nil {
		select {
		case <-c.ctx.Done():
			return
		default:

		}
		c.dial()
	}
}

func (c *Client) SendMsg(MsgID uint32, Data []byte) error {
	return c.sendMsgContext(context.Background(), MsgID, Data)
}

func (c *Client) SendMsgContext(ctx context.Context, MsgID uint32, Data []byte) error {
	return c.sendMsgContext(ctx, MsgID, Data)
}

func (c *Client) sendMsgContext(ctx context.Context, MsgID uint32, Data []byte) error {
	conn := c.connPointer.Load()
	if conn == nil {
		return ErrConn
	}
	return conn.SendMsgContext(ctx, MsgID, Data)
}

func (c *Client) dial() {
	if conn, data, err := c.action.DialContext(c.ctx); err != nil {
		return
	} else {
		defer conn.Close()
		handlerManager := connection.NewHandlerManager(
			conn,
			c.handleMsg,
			c.maxDataLen,
			c.action.ConnectedBegin,
			data,
		)
		defer func() {
			<-handlerManager.Stop()
			c.action.ConnErr(c.ctx, handlerManager.GetConnection(), handlerManager.Err())
		}()

		c.connPointer.Store(handlerManager.GetConnection())

		select {
		case <-c.ctx.Done():
			return
		case <-handlerManager.Ctx().Done():
			return
		}

	}
}
