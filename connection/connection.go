package connection

import (
	"context"
	"fmt"
	"sync"

	"github.com/jursonmo/simple-message/protocol"
)

type Connection struct {
	conn     Conn
	msgChan  chan *MessageBody
	ctx      context.Context
	cancel   context.CancelFunc
	property sync.Map
	data     any
}

func NewConnection(conn Conn, data any) *Connection {
	C := &Connection{
		msgChan: make(chan *MessageBody, 256),
		conn:    conn,
		data:    data,
	}
	C.ctx, C.cancel = context.WithCancel(context.Background())
	return C
}

func (c *Connection) String() string {
	return fmt.Sprintf("%s-%v, data: %v", c.conn.LocalAddr().String(), c.conn.RemoteAddr().String(), c.data)
}

func (C *Connection) MsgChan() <-chan *MessageBody {
	return C.msgChan
}

func (C *Connection) GetData() any {
	return C.data
}

func (C *Connection) StoreProperty(k string, v interface{}) {
	C.property.Store(k, v)
}

func (C *Connection) LoadProperty(k string) (interface{}, bool) {
	return C.property.Load(k)
}

func (C *Connection) DeleteProperty(key string) {
	C.property.Delete(key)
}

func (C *Connection) ClearProperty() {
	C.property.Clear()
}

func (C *Connection) CompareAndDeleteProperty(key, old any) (deleted bool) {
	return C.property.CompareAndDelete(key, old)
}

func (C *Connection) CompareAndSwapProperty(key, old, new any) (swapped bool) {
	return C.property.CompareAndSwap(key, old, new)
}

func (C *Connection) LoadAndDeleteProperty(key any) (value any, loaded bool) {
	return C.property.LoadAndDelete(key)
}

func (C *Connection) LoadOrStoreProperty(key, value any) (actual any, loaded bool) {
	return C.property.LoadOrStore(key, value)
}

func (C *Connection) RangeProperty(f func(key, value any) bool) {
	C.property.Range(f)
}

func (C *Connection) SwapProperty(key, value any) (previous any, loaded bool) {
	return C.property.Swap(key, value)
}

func (C *Connection) SendMsgContext(ctx context.Context, MsgID uint32, Data []byte) error {
	return C.sendMsg(ctx, MsgID, Data)
}

func (C *Connection) SendMsg(MsgID uint32, Data []byte) error {
	return C.sendMsg(context.TODO(), MsgID, Data)
}

func (C *Connection) sendMsg(ctx context.Context, MsgID uint32, Data []byte) error {
	m := NewMessageBody(&protocol.Message{
		MsgID: MsgID,
		Data:  Data,
	})
	select {
	case <-C.ctx.Done():
		return ErrIsClose
	case <-ctx.Done():
		return ctx.Err()
	case C.msgChan <- m:
		<-m.ackChan
		return m.err
	}
}

func (C *Connection) Close() {
	C.cancel() // 取消上下文, 通知所有发送消息的goroutine send() 退出, 对应的handlerManager 会stop() --> cancel, 从而停止所有任务.
}

func (C *Connection) Ctx() context.Context {
	return C.ctx
}
