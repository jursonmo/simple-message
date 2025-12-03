package websocket

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/jursonmo/simple-message/connection" // 替换为你的 connection 包路径
)

// WebSocketListener 实现 Listener 接口，用于监听 WebSocket 连接
type WebSocketListener struct {
	server    *http.Server       // HTTP 服务器实例
	listener  net.Listener       // 底层 TCP 监听器
	upgrader  websocket.Upgrader // WebSocket 升级器
	connChan  chan *Conn         // 连接通道
	closeChan chan struct{}      // 关闭通道
	mu        sync.Mutex         // 互斥锁
	closed    bool               // 是否已关闭
}

// NewWebSocketListener 创建 WebSocket 监听器
// addr: 监听地址（如 ":8080"）
// path: WebSocket 路径（如 "/ws"）
func NewWebSocketListener(addr, path string) (*WebSocketListener, error) {
	// 创建 TCP 监听器
	tcpListener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	wsListener := &WebSocketListener{
		listener: tcpListener,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // 生产环境需验证 Origin
			},
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
		connChan:  make(chan *Conn, 100),
		closeChan: make(chan struct{}),
	}

	// 启动 HTTP 服务器处理 WebSocket 升级
	mux := http.NewServeMux()
	mux.HandleFunc(path, wsListener.handleWS)
	wsListener.server = &http.Server{Handler: mux}

	// 异步启动 HTTP 服务器
	go func() {
		if err := wsListener.server.Serve(tcpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// 非关闭导致的错误，关闭监听器
			wsListener.Close()
		}
	}()

	return wsListener, nil
}

// handleWS 处理 WebSocket 升级请求
func (l *WebSocketListener) handleWS(w http.ResponseWriter, r *http.Request) {
	// 升级 HTTP 连接为 WebSocket
	conn, err := l.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	///这个地方可以进行握手的操作
	wrapper, ctx := NewWebSocketConn(conn)
	defer wrapper.Close()

	// 将连接发送到 Accept 通道
	select {
	case <-ctx.Done():
		return
	case l.connChan <- wrapper:
		select {
		case <-ctx.Done():
		case <-l.closeChan:

		}
		fmt.Printf("断开")
		return
	case <-l.closeChan:

	}
}

// Accept 实现 Listener 接口，等待并返回新的 WebSocket 连接
func (l *WebSocketListener) Accept() (connection.Conn, any, error) {
	select {
	case conn := <-l.connChan:
		// 返回连接、自定义数据（这里返回请求头信息示例）
		return conn, map[string]string{"type": "websocket"}, nil
	case <-l.closeChan:
		return nil, nil, net.ErrClosed
	}
}

// Close 实现 Listener 接口，关闭监听器
func (l *WebSocketListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}
	l.closed = true

	close(l.closeChan)
	l.server.Close()   // 关闭 HTTP 服务器
	l.listener.Close() // 关闭底层 TCP 监听器
	close(l.connChan)  // 关闭连接通道

	return nil
}
