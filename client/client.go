package client

import (
	"context"
	"errors"
	"maps"
	"sync/atomic"

	"github.com/jursonmo/simple-message/connection"
)

var (
	ErrIsClose = errors.New("已关闭")
	ErrConn    = errors.New("连接失败")
)

type Client struct {
	handler     map[uint32]connection.Handler
	ctx         context.Context
	cancel      context.CancelFunc
	maxDataLen  uint32
	action      Action
	done        chan struct{}
	connPointer atomic.Pointer[connection.Connection]
}

func NewClient(
	handler map[uint32]connection.Handler,
	maxDataLen uint32,
	action Action,
) *Client {
	c := &Client{
		handler:    maps.Clone(handler),
		action:     action,
		maxDataLen: maxDataLen,
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())

	c.done = make(chan struct{})
	go func() {
		defer close(c.done)
		c.start()
		c.cancel()
	}()

	return c
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
			c.handler,
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
