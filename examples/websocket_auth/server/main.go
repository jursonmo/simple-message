package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jursonmo/simple-message/connection"
	www "github.com/jursonmo/simple-message/examples/websocket"
	"github.com/jursonmo/simple-message/server"
)

const (
	listenAddr = ":18081"
	wsPath     = "/auth"

	msgHello    uint32 = 1
	msgAuthReq  uint32 = 1000
	msgAuthResp uint32 = 1001

	authKey     = "auth_session"
	authTimeout = 5 * time.Second
)

type AuthRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type AuthResponse struct {
	OK      bool   `json:"ok"`
	Message string `json:"message"`
	User    string `json:"user,omitempty"`
}

// AuthSession 是认证成功后绑定到连接上的会话信息。
// 后续业务 handler 不需要重新解析认证消息，直接从连接属性读取即可。
type AuthSession struct {
	Username string
	LoginAt  time.Time
}

var users = map[string]string{
	"admin": "123456",
	"will":  "pass123",
}

type AuthHandler struct{}

func (h *AuthHandler) Handle(request connection.IRequest) error {
	conn := request.GetConnection()

	if _, ok := conn.LoadProperty(authKey); ok {
		return sendAuthResponse(conn, true, "already authenticated", "")
	}

	var authReq AuthRequest
	if err := json.Unmarshal(request.GetData(), &authReq); err != nil {
		_ = sendAuthResponse(conn, false, "invalid auth json", "")
		conn.Close()
		return fmt.Errorf("认证请求格式错误: %w", err)
	}

	if users[authReq.Username] != authReq.Password {
		_ = sendAuthResponse(conn, false, "invalid username or password", "")
		conn.Close()
		return fmt.Errorf("用户名或密码错误: %s", authReq.Username)
	}

	// 认证成功后把会话状态保存在 Connection 上，业务 handler 通过 RequireAuth 统一读取。
	session := &AuthSession{
		Username: authReq.Username,
		LoginAt:  time.Now(),
	}
	conn.StoreProperty(authKey, session)

	log.Printf("WebSocket 认证成功: user=%s, conn=%v", authReq.Username, conn)
	return sendAuthResponse(conn, true, "auth ok", authReq.Username)
}

// AuthRequired 是一个 handler 包装器，用来保护业务消息。
// 这样每个业务 handler 不需要重复写认证检查逻辑。
type AuthRequired struct {
	next connection.Handler
}

func RequireAuth(next connection.Handler) connection.Handler {
	return &AuthRequired{next: next}
}

func (h *AuthRequired) Handle(request connection.IRequest) error {
	if _, ok := request.GetConnection().LoadProperty(authKey); !ok {
		_ = sendAuthResponse(request.GetConnection(), false, "please auth first", "")
		request.GetConnection().Close()
		return errors.New("未认证连接不能访问业务消息")
	}

	return h.next.Handle(request)
}

type HelloHandler struct{}

func (h *HelloHandler) Handle(request connection.IRequest) error {
	session, _ := request.GetConnection().LoadProperty(authKey)
	authSession, _ := session.(*AuthSession)

	username := "unknown"
	if authSession != nil {
		username = authSession.Username
	}

	log.Printf("收到 WebSocket 业务消息: user=%s, msgID=%d, data=%s", username, request.GetMsgID(), string(request.GetData()))

	reply := fmt.Sprintf("hello %s, websocket server received: %s", username, string(request.GetData()))
	if err := request.GetConnection().SendMsg(msgHello, []byte(reply)); err != nil {
		log.Printf("发送 WebSocket 业务响应失败: %v", err)
		return err
	}

	return nil
}

type Action struct{}

func (a *Action) ConnErr(ctx context.Context, conn *connection.Connection, err error) {
	log.Printf("WebSocket 连接断开: %v, 连接信息: %v", err, conn)
}

func (a *Action) ConnectedBegin(ctx context.Context, conn *connection.Connection) {
	log.Printf("WebSocket 新连接建立，等待认证: %v", conn)

	// 防止客户端完成 WebSocket 握手后迟迟不发送认证消息。
	// 超时只关闭当前连接，不影响 listener 继续接受其他 WebSocket 连接。
	go func() {
		timer := time.NewTimer(authTimeout)
		defer timer.Stop()

		select {
		case <-timer.C:
			if _, ok := conn.LoadProperty(authKey); !ok {
				log.Printf("WebSocket 认证超时，关闭连接: %v", conn)
				conn.Close()
			}
		case <-conn.Ctx().Done():
		case <-ctx.Done():
		}
	}()
}

func sendAuthResponse(conn *connection.Connection, ok bool, message, username string) error {
	data, err := json.Marshal(AuthResponse{
		OK:      ok,
		Message: message,
		User:    username,
	})
	if err != nil {
		return err
	}

	return conn.SendMsg(msgAuthResp, data)
}

func main() {
	customListener, err := www.NewWebSocketListener(listenAddr, wsPath)
	if err != nil {
		log.Printf("创建 WebSocket listener 失败: %v", err)
		return
	}

	handlers := map[uint32]connection.Handler{
		msgAuthReq: &AuthHandler{},
		msgHello:   RequireAuth(&HelloHandler{}),
	}

	srv := server.NewServer(
		customListener,
		new(Action),
		server.WithHandlers(handlers),
		server.WithMaxDataLen(1024*1024),
	)

	ctx := context.Background()
	done := srv.Start(ctx, 16)

	defer func() {
		log.Println("正在关闭 WebSocket auth 服务器...")
		srv.Stop()
		<-done
		log.Println("WebSocket auth 服务器已完全停止")
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("WebSocket auth 服务器已启动: ws://127.0.0.1%s%s", listenAddr, wsPath)
	log.Println("按 Ctrl+C 停止服务器")

	select {
	case <-done:
		log.Println("服务器正常退出")
	case sig := <-signalChan:
		log.Printf("收到退出信号: %v，正在停止服务器...", sig)
	}
}
