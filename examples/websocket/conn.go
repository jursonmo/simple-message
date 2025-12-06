package websocket

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/gorilla/websocket"
)

// WebSocketRWCloser 封装 websocket.Conn 实现 io.ReadWriteCloser
type Conn struct {
	conn     *websocket.Conn // 底层 WebSocket 连接
	msgType  int             // 消息类型（TextMessage/BinaryMessage）
	readBuf  []byte          // 读取缓冲区（缓存未读完的消息）
	mu       sync.Mutex      // 互斥锁（保证并发安全）
	closed   bool            // 连接是否已关闭
	readErr  error           // 读取错误（用于终止循环）
	writeErr error           // 写入错误（用于终止循环）
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewWebSocketRWCloser 创建封装实例
// msgType: websocket.TextMessage 或 websocket.BinaryMessage
func NewWebSocketConn(conn *websocket.Conn) (*Conn, context.Context) {
	ctx, cancel := context.WithCancel(context.Background())
	return &Conn{
		conn:    conn,
		msgType: websocket.BinaryMessage,
		ctx:     ctx,
		cancel:  cancel,
	}, ctx
}

// Read 从 WebSocket 读取数据（适配 io.Reader）
// 注意：WebSocket 是消息级协议，Read 会按消息分割，需缓存未读完的消息
func (w *Conn) Read(p []byte) (n int, err error) {
	w.mu.Lock()

	if w.closed {
		w.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if w.readErr != nil {
		w.mu.Unlock()
		return 0, w.readErr
	}

	// 优先读取缓冲区剩余数据
	if len(w.readBuf) > 0 {
		n = copy(p, w.readBuf)
		w.readBuf = w.readBuf[n:]
		w.mu.Unlock()
		return n, nil
	}

	w.mu.Unlock()

	// 缓冲区为空，读取下一条消息
	_, msg, err := w.conn.ReadMessage()
	w.mu.Lock()
	defer w.mu.Unlock()
	if err != nil {
		w.readErr = err
		// 区分正常关闭和错误
		if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
			return 0, io.EOF
		}
		return 0, err
	}

	// 将消息内容写入输出缓冲区
	n = copy(p, msg)
	// 缓存未读完的部分
	if n < len(msg) {
		w.readBuf = msg[n:]
	}

	return n, nil
}

// Write 向 WebSocket 写入数据（适配 io.Writer）
// 注意：WebSocket 按消息发送，每次 Write 会封装为一条完整消息
func (w *Conn) Write(p []byte) (n int, err error) {
	w.mu.Lock()

	if w.closed {
		w.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if w.writeErr != nil {
		w.mu.Unlock()
		return 0, w.writeErr
	}
	w.mu.Unlock()

	// 发送消息（完整写入 p）
	err = w.conn.WriteMessage(w.msgType, p)
	w.mu.Lock()
	defer w.mu.Unlock()

	if err != nil {
		w.writeErr = err
		return 0, err
	}

	return len(p), nil
}

// Close 关闭连接（适配 io.Closer）
func (w *Conn) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.cancel()

	if w.closed {
		return nil
	}
	w.closed = true

	// 发送关闭帧（优雅关闭）
	err := w.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	if err != nil && !errors.Is(err, websocket.ErrCloseSent) {
		return err
	}
	// 关闭底层连接
	return w.conn.Close()
}

func (w *Conn) LocalAddr() net.Addr {
	return w.conn.LocalAddr()
}

func (w *Conn) RemoteAddr() net.Addr {
	return w.conn.RemoteAddr()
}
