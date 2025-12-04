package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorilla/websocket"
	"github.com/jursonmo/simple-message/client"
	"github.com/jursonmo/simple-message/connection"
	www "github.com/jursonmo/simple-message/examples/websocket"
)

// Handler1 消息处理器，用于处理MsgID=1的消息
type Handler1 struct{}

// Handle 实现消息处理接口，打印收到的消息ID和内容
func (h *Handler1) Handle(request connection.IRequest) error {
	fmt.Printf("收到消息 - ID: %d, 内容: %s\n",
		request.GetMsgID(),
		string(request.GetData()))
	return nil
}

type Action struct{}

func (a *Action) DialContext(ctx context.Context) (connection.Conn, any, error) {
	conn, _, err := websocket.DefaultDialer.Dial("ws://localhost:18080/test", nil)
	if err != nil {
		fmt.Printf("websocket连接错误: %v, 准备重连...\n", err)
		return nil, nil, err
	}

	wConn, _ := www.NewWebSocketConn(conn)

	///进行一些握手之类的操作
	return wConn, "token", nil
}

// 拨号错误回调 - 返回新的拨号函数用于重连
func (a *Action) DialErr(ctx context.Context, err error) {
	fmt.Printf("拨号错误: %v, 准备重连...\n", err)
}

func (a *Action) ConnErr(ctx context.Context, conn *connection.Connection, err error) {
	fmt.Printf("连接错误: %v, 连接信息: %v, 准备重连...\n", err, conn)
}

func (a *Action) ConnectedBegin(ctx context.Context, conn *connection.Connection) {
	fmt.Printf("成功连接到服务器: %v\n", conn)
}

func main() {
	// 注册消息处理器，MsgID=1对应Handler1
	handlers := map[uint32]connection.Handler{
		1: &Handler1{},
	}

	// 创建客户端实例
	c := client.NewClient(
		new(Action),
		client.WithHandlers(handlers),    // 消息处理器映射
		client.WithMaxDataLen(1024*1024), // 最大数据长度 (1MB)
	)

	// 启动客户端
	c.Start(context.Background())

	// 确保程序退出时正确停止客户端
	defer func() {
		fmt.Println("正在关闭客户端...")
		// 等待客户端完全停止
		<-c.Stop()
		fmt.Println("客户端已完全停止")
	}()

	// 设置信号监听，处理程序退出
	signalChan := make(chan os.Signal, 1)
	signal.Notify(
		signalChan,
		syscall.SIGINT,  // Ctrl+C中断
		syscall.SIGTERM, // 终止信号
		//os.Kill,         // 强制终止
	)

	fmt.Println("客户端已启动，正在连接到服务器 127.0.0.1:2000...")
	fmt.Println("按Ctrl+C停止客户端")

	// 等待退出信号
	<-signalChan
	fmt.Println("收到退出信号，正在停止客户端...")
}
