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

	"github.com/jursonmo/simple-message/client"
	"github.com/jursonmo/simple-message/connection"
)

const (
	serverAddr = "127.0.0.1:2001"

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
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", serverAddr)
	if err != nil {
		log.Printf("dial失败: %v", err)
		return nil, nil, fmt.Errorf("连接失败: %w", err)
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetKeepAlive(true); err != nil {
			log.Printf("设置TCP保活失败: %v", err)
		}
	}

	return conn, nil, nil
}

func (a *Action) ConnErr(ctx context.Context, conn *connection.Connection, err error) {
	log.Printf("连接断开: %v, 连接信息: %v，准备重连...", err, conn)
}

func (a *Action) ConnectedBegin(ctx context.Context, conn *connection.Connection) {
	log.Printf("成功连接到服务器: %v", conn)

	data, err := json.Marshal(AuthRequest{
		Username: a.username,
		Password: a.password,
	})
	if err != nil {
		log.Printf("构造认证请求失败: %v", err)
		conn.Close()
		return
	}

	if err := conn.SendMsg(msgAuthReq, data); err != nil {
		log.Printf("发送认证请求失败: %v", err)
		conn.Close()
		return
	}

	log.Printf("认证请求已发送: user=%s", a.username)
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

	if err := request.GetConnection().SendMsg(msgHello, []byte("hello after auth")); err != nil {
		log.Printf("发送业务消息失败: %v", err)
		return err
	}

	log.Println("认证后业务消息已发送")
	return nil
}

type HelloHandler struct{}

func (h *HelloHandler) Handle(request connection.IRequest) error {
	log.Printf("收到服务器业务响应: msgID=%d, data=%s", request.GetMsgID(), string(request.GetData()))

	time.Sleep(time.Second)
	if err := request.GetConnection().SendMsg(msgHello, []byte("client heartbeat after auth")); err != nil {
		log.Printf("发送业务消息失败: %v", err)
		return err
	}

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
		log.Println("正在关闭客户端...")
		cancel()
		<-c.Stop()
		log.Println("客户端已完全停止")
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	log.Println("tcp_auth 客户端已启动，正在连接到服务器 127.0.0.1:2001...")
	log.Println("按 Ctrl+C 停止客户端")

	<-signalChan
	log.Println("收到退出信号，正在停止客户端...")
}
