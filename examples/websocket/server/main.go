package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jursonmo/simple-message/connection"
	www "github.com/jursonmo/simple-message/examples/websocket"
	"github.com/jursonmo/simple-message/server"
)

// Handler1 消息处理器，用于处理MsgID=1的消息
type Handler1 struct{}

// Handle 实现消息处理接口
func (h *Handler1) Handle(request connection.IRequest) error {
	// 可以通过以下方法获取消息数据和ID
	// data := request.GetData()
	// msgID := request.GetMsgID()

	// 此处可添加消息处理逻辑
	return nil
}

type Action struct{}

// 连接错误回调
func (l *Action) ConnErr(ctx context.Context, conn *connection.Connection, err error) {
	fmt.Printf("连接错误: %v, 连接信息: %v\n", err, conn)
}

// 连接建立回调
func (l *Action) ConnectedBegin(ctx context.Context, conn *connection.Connection) {
	fmt.Printf("新连接建立: %v\n", conn)
	// 向新连接发送欢迎消息
	if err := conn.SendMsg(1, []byte("欢迎连接到服务器")); err != nil {
		fmt.Printf("发送消息失败: %v\n", err)
	}
}

func main() {
	customListener, err := www.NewWebSocketListener(":18080", "/test")
	if err != nil {
		log.Println("创建websocket失败:", err)
		return
	}

	// 注册消息处理器，MsgID=1对应Handler1
	handlers := map[uint32]connection.Handler{
		1: &Handler1{},
	}

	// 创建服务器实例
	srv := server.NewServer(
		customListener,
		new(Action),
		server.WithHandlers(handlers),
	)

	// 启动服务器，使用16个accept协程
	ctx, cancel := context.WithCancel(context.Background())
	_ = cancel //TODO: 取消ctx时，应该可以关闭server
	done := srv.Start(ctx, 16)
	defer func() {
		// 停止服务器并等待完成
		srv.Stop()
		<-done
		fmt.Println("服务器已完全停止")
	}()

	// 设置信号监听，处理程序退出
	signalChan := make(chan os.Signal, 1)
	signal.Notify(
		signalChan,
		syscall.SIGINT,  // Ctrl+C中断
		syscall.SIGTERM, // 终止信号
		//os.Kill,         // 强制终止
	)

	fmt.Println("服务器已启动，监听端口: 18080")
	fmt.Println("按Ctrl+C停止服务器")

	// 等待退出信号或服务器完成信号
	select {
	case <-done:
		fmt.Println("服务器正常退出")
	case sig := <-signalChan:
		fmt.Printf("收到退出信号: %v，正在停止服务器...\n", sig)
	}
}
