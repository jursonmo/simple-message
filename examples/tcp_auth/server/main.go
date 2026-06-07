package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jursonmo/simple-message/connection"
	"github.com/jursonmo/simple-message/server"
)

const (
	addr = ":2001"

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

type AuthSession struct {
	Username string
	LoginAt  time.Time
}

var users = map[string]string{
	"admin": "123456",
	"will":  "pass123",
}

type Listener struct {
	listener net.Listener
}

func (l *Listener) Accept() (connection.Conn, any, error) {
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Println("监听器已关闭，正常退出")
				return nil, nil, err
			}

			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				log.Printf("临时错误: %v，将重试", err)
				continue
			}

			return nil, nil, err
		}

		// 这里只接入 TCP，不等待认证数据，避免慢连接阻塞 Accept。
		return conn, nil, nil
	}
}

func (l *Listener) Close() error {
	return l.listener.Close()
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

	session := &AuthSession{
		Username: authReq.Username,
		LoginAt:  time.Now(),
	}
	conn.StoreProperty(authKey, session)

	log.Printf("认证成功: user=%s, conn=%v", authReq.Username, conn)
	return sendAuthResponse(conn, true, "auth ok", authReq.Username)
}

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

	log.Printf("收到业务消息: user=%s, msgID=%d, data=%s", username, request.GetMsgID(), string(request.GetData()))

	reply := fmt.Sprintf("hello %s, server received: %s", username, string(request.GetData()))
	if err := request.GetConnection().SendMsg(msgHello, []byte(reply)); err != nil {
		log.Printf("发送业务响应失败: %v", err)
		return err
	}

	return nil
}

type Action struct{}

func (a *Action) ConnErr(ctx context.Context, conn *connection.Connection, err error) {
	log.Printf("连接断开: %v, 连接信息: %v", err, conn)
}

func (a *Action) ConnectedBegin(ctx context.Context, conn *connection.Connection) {
	log.Printf("新连接建立，等待认证: %v", conn)

	go func() {
		timer := time.NewTimer(authTimeout)
		defer timer.Stop()

		select {
		case <-timer.C:
			if _, ok := conn.LoadProperty(authKey); !ok {
				log.Printf("认证超时，关闭连接: %v", conn)
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
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("创建监听器失败: %v", err)
		return
	}
	defer listener.Close()

	handlers := map[uint32]connection.Handler{
		msgAuthReq: &AuthHandler{},
		msgHello:   RequireAuth(&HelloHandler{}),
	}

	srv := server.NewServer(
		&Listener{listener: listener},
		new(Action),
		server.WithHandlers(handlers),
		server.WithMaxDataLen(1024*1024),
	)

	ctx := context.Background()
	done := srv.Start(ctx, 16)

	defer func() {
		log.Println("正在关闭服务器...")
		srv.Stop()
		<-done
		log.Println("服务器已完全停止")
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("tcp_auth 服务器已启动，监听端口: 2001")
	log.Println("按 Ctrl+C 停止服务器")

	select {
	case <-done:
		log.Println("服务器正常退出")
	case sig := <-signalChan:
		log.Printf("收到退出信号: %v，正在停止服务器...", sig)
	}
}
