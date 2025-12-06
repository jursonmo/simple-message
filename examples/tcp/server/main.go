package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/jursonmo/simple-message/connection"
	"github.com/jursonmo/simple-message/server"
)

// Handler1 消息处理器，用于处理MsgID=1的消息
type Handler1 struct{}

// Handle 实现消息处理接口
func (h *Handler1) Handle(request connection.IRequest) error {
	// 可以通过以下方法获取消息数据和ID
	data := request.GetData()
	msgID := request.GetMsgID()
	// 打印收到的消息
	log.Printf("收到消息 - MsgID: %d, Data: %s\n", msgID, string(data))

	// 此处可添加消息处理逻辑
	// 模拟处理延迟
	time.Sleep(1 * time.Second)

	// 向客户端发送确认消息（MsgID=1）
	if err := request.GetConnection().SendMsg(1, []byte("hello from server")); err != nil {
		log.Printf("发送确认消息失败: %v\n", err)
		return err
	}
	log.Printf("确认消息已发送 - MsgID: %d\n", msgID)

	// 向客户端发送确认消息（MsgID=2）
	if err := request.GetConnection().SendMsg(2, []byte("hello from server")); err != nil {
		log.Printf("发送确认消息失败: %v\n", err)
		return err
	}
	log.Printf("确认消息已发送 - MsgID: %d\n", 2)
	return nil
}

// Listener 自定义监听器实现，包装net.Listener
type Listener struct {
	listener net.Listener
}

// Accept 实现Accept接口，返回读写关闭器
func (l *Listener) Accept() (connection.Conn, any, error) {
	for {
		if conn, err := l.listener.Accept(); err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Println("监听器已关闭，正常退出")
				return nil, nil, err
			}

			// 2. 判断是否为临时错误（可重试）
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				log.Printf("临时错误: %v，将重试\n", err)
				continue
			}

		} else {
			///进行一些握手之类的操作
			return conn, "token", nil
		}
	}
}

// Close 实现关闭接口
func (l *Listener) Close() error {
	return l.listener.Close()
}

type Action struct{}

// 连接错误回调
func (l *Action) ConnErr(ctx context.Context, conn *connection.Connection, err error) {
	log.Printf("连接断开: %v, 连接信息: %v\n", err, conn)
}

// 连接建立回调
func (l *Action) ConnectedBegin(ctx context.Context, conn *connection.Connection) {
	log.Printf("新连接建立: %v\n", conn)
	// 向新连接发送欢迎消息
	if err := conn.SendMsg(1, []byte("欢迎连接到服务器")); err != nil {
		log.Printf("发送消息失败: %v\n", err)
	}
}

func main() {
	// 创建TCP监听器，监听2000端口
	listener, err := net.Listen("tcp", ":2000")
	if err != nil {
		log.Printf("创建监听器失败: %v\n", err)
		return
	}
	defer listener.Close()

	// 注册消息处理器，MsgID=1对应Handler1
	handlers := map[uint32]connection.Handler{
		1: &Handler1{},
	}

	// 包装自定义监听器
	customListener := &Listener{
		listener: listener,
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
		log.Println("关闭前, goroutine 数量:", runtime.NumGoroutine())
		srv.Stop()
		<-done
		log.Println("服务器已完全停止")
		log.Println("关闭后, goroutine 数量:", runtime.NumGoroutine())
	}()
	// 设置信号监听，处理程序退出
	signalChan := make(chan os.Signal, 1)
	signal.Notify(
		signalChan,
		syscall.SIGINT,  // Ctrl+C中断
		syscall.SIGTERM, // 终止信号
		//os.Kill,       // 强制终止
	)

	log.Println("服务器已启动，监听端口: 2000")
	log.Println("按Ctrl+C停止服务器")

	// 等待退出信号或服务器完成信号
	select {
	case <-done:
		log.Println("服务器正常退出")
	case sig := <-signalChan:
		log.Printf("收到退出信号: %v，正在停止服务器...\n", sig)
	}
}
