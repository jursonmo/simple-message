package client

import (
	"context"
	"errors"
	"log"
	"maps"
	"sync"
	"sync/atomic"
	"time"

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
	stoped      atomic.Bool
	connPointer atomic.Pointer[connection.Connection] //atomic.Value

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

// 为了一致性，只由cancel context 来停止server，Stop() 其实就是封装调用 cancel 方法而已。
func (c *Client) Stop() <-chan struct{} {
	if c.cancel != nil {
		c.cancel()
	}
	return c.done
}

func (c *Client) IsStoped() bool {
	return c.stoped.Load()
}

func (c *Client) start() {
	var dialStartAt time.Time
	for c.action != nil {
		select {
		case <-c.ctx.Done():
			c.stoped.Store(true)
			return
		default:

		}

		dialStartAt = time.Now()
		c.dialAndRun()
		log.Printf("client dial and run cost: %v\n", time.Since(dialStartAt))
		//为了避免过于频繁的拨号，设置最小拨号间隔为1秒, 至少等待1秒, 除非ctx 被cancel.(比如server 没有启动，dial() 会立即返回connection refused的错误)
		SleepAtLeast(c.ctx, dialStartAt, time.Second)
	}
}

// SleepAtLeast 确保在 ctx 取消前，至少 sleep  sleepAtLeast 时间
func SleepAtLeast(ctx context.Context, start time.Time, sleepAtLeast time.Duration) {
	elapsed := time.Since(start) // Go 1.9 引入了单调时钟（monotonic clock）支持, 确保elapsed不会是负数。
	//保险措施，还是判断一下
	if elapsed < 0 {
		return
	}
	if elapsed >= sleepAtLeast {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, sleepAtLeast-elapsed)
	defer cancel()
	<-ctx.Done()
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

func (c *Client) dialAndRun() {
	conn, data, err := c.action.DialContext(c.ctx)
	if err != nil {
		return
	}

	defer conn.Close()
	handlerManager := connection.NewHandlerManager(
		c.ctx,
		conn,
		c.handleMsg,
		c.maxDataLen,
		c.action.ConnectedBegin,
		data,
	)
	defer func() {
		log.Println("handlerManager stop now")
		//<-handlerManager.Stop()
		//handlerManager.MustStopWithTimeout(time.Second * 2)
		handlerManager.StopWithTimeout(time.Second * 2)
		c.action.ConnErr(c.ctx, handlerManager.GetConnection(), handlerManager.Err())
		log.Println("handlerManager stop done")
	}()

	c.connPointer.Store(handlerManager.GetConnection())

	select {
	// case <-c.ctx.Done():  //ctx 取消时, handlerManager.Ctx() 也会取消, 只需保留 handlerManager.Ctx().Done() 即可
	// 	return
	case <-handlerManager.Ctx().Done():
		return
	}
}
