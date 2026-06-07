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

	"github.com/gorilla/websocket"
	"github.com/jursonmo/simple-message/client"
	"github.com/jursonmo/simple-message/connection"
	www "github.com/jursonmo/simple-message/examples/websocket"
)

const (
	serverURL = "ws://127.0.0.1:18081/auth"

	msgHello    uint32 = 1
	msgAuthReq  uint32 = 1000
	msgAuthResp uint32 = 1001
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

type Action struct {
	username string
	password string
}

func (a *Action) DialContext(ctx context.Context) (connection.Conn, any, error) {
	// 这里仅完成 WebSocket 握手，并把 gorilla/websocket 连接包装为 connection.Conn。
	// 用户名密码不放在 URL 或 HTTP header 中，统一走 simple-message 的认证消息。
	wsConn, _, err := websocket.DefaultDialer.DialContext(ctx, serverURL, nil)
	if err != nil {
		log.Printf("WebSocket 连接失败: %v", err)
		return nil, nil, err
	}

	conn, _ := www.NewWebSocketConn(wsConn)
	return conn, nil, nil
}

func (a *Action) ConnErr(ctx context.Context, conn *connection.Connection, err error) {
	log.Printf("WebSocket 连接断开: %v, 连接信息: %v，准备重连...", err, conn)
}

func (a *Action) ConnectedBegin(ctx context.Context, conn *connection.Connection) {
	log.Printf("成功连接到 WebSocket 服务器: %v", conn)

	data, err := json.Marshal(AuthRequest{
		Username: a.username,
		Password: a.password,
	})
	if err != nil {
		log.Printf("构造认证请求失败: %v", err)
		conn.Close()
		return
	}

	// 连接建立后第一条消息必须是认证请求；认证成功响应由 AuthRespHandler 处理。
	if err := conn.SendMsg(msgAuthReq, data); err != nil {
		log.Printf("发送认证请求失败: %v", err)
		conn.Close()
		return
	}

	log.Printf("WebSocket 认证请求已发送: user=%s", a.username)
}

type AuthRespHandler struct{}

func (h *AuthRespHandler) Handle(request connection.IRequest) error {
	var resp AuthResponse
	if err := json.Unmarshal(request.GetData(), &resp); err != nil {
		log.Printf("解析认证响应失败: %v", err)
		request.GetConnection().Close()
		return err
	}

	if !resp.OK {
		log.Printf("认证失败: %s", resp.Message)
		request.GetConnection().Close()
		return errors.New(resp.Message)
	}

	log.Printf("认证成功: user=%s, message=%s", resp.User, resp.Message)

	// 收到认证成功后再发送业务消息，避免未认证业务请求被服务端拒绝。
	if err := request.GetConnection().SendMsg(msgHello, []byte("hello websocket after auth")); err != nil {
		log.Printf("发送业务消息失败: %v", err)
		return err
	}

	log.Println("认证后 WebSocket 业务消息已发送")
	return nil
}

type HelloHandler struct{}

func (h *HelloHandler) Handle(request connection.IRequest) error {
	fmt.Printf("收到 WebSocket 服务端业务响应 - ID: %d, 内容: %s\n",
		request.GetMsgID(),
		string(request.GetData()))
	return nil
}

func main() {
	handlers := map[uint32]connection.Handler{
		msgAuthResp: &AuthRespHandler{},
		msgHello:    &HelloHandler{},
	}

	c := client.NewClient(
		&Action{
			username: "admin",
			password: "123456",
		},
		client.WithHandlers(handlers),
		client.WithMaxDataLen(1024*1024),
	)

	ctx, cancel := context.WithCancel(context.Background())
	c.Start(ctx)

	defer func() {
		log.Println("正在关闭 WebSocket auth 客户端...")
		cancel()
		<-c.Stop()
		log.Println("WebSocket auth 客户端已完全停止")
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("WebSocket auth 客户端已启动，正在连接 %s", serverURL)
	log.Println("按 Ctrl+C 停止客户端")

	<-signalChan
	log.Println("收到退出信号，正在停止客户端...")
}
