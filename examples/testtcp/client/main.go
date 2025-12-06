package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/jursonmo/simple-message/client"
	"github.com/jursonmo/simple-message/connection"
)

// Handler1 消息处理器，用于处理MsgID=1的消息
type Handler1 struct{}

// Handle 实现消息处理接口，打印收到的消息ID和内容
func (h *Handler1) Handle(request connection.IRequest) error {
	log.Printf("Handler1 收到消息 - ID: %d, 内容: %s\n", request.GetMsgID(), string(request.GetData()))

	time.Sleep(time.Second)
	// 从请求中获取连接实例
	conn := request.GetConnection()
	// 发送消息 - MsgID=1, 内容="hello from client"
	if err := conn.SendMsg(1, []byte("hello from client")); err != nil {
		log.Printf("发送消息失败: %v\n", err)
		return err
	}
	log.Printf("消息已发送 - MsgID: %d, 内容: %s\n", 1, "hello from client")
	return nil
}

// Handler2 消息处理器，用于处理MsgID=2的消息
type Handler2 struct{}

var HaveRemoveHandler2 = false // 用于标记是否已移除Handler2, 用于测试是否会收到移除后的消息
var TestCloseConn sync.Once

// Handle 实现消息处理接口，打印收到的消息ID和内容
func (h *Handler2) Handle(request connection.IRequest) error {
	log.Printf("Handler2 收到消息 - ID: %d, 内容: %s\n", request.GetMsgID(), string(request.GetData()))
	if HaveRemoveHandler2 {
		log.Printf("Handler2 已被移除，但是依然收到了消息 - ID: %d, 内容: %s\n", request.GetMsgID(), string(request.GetData()))
		panic("Handler2 已被移除，应该不能收到消息")
	}
	// 测试关闭连接,看能不能让客户端停止并重连
	TestCloseConn.Do(func() {
		log.Printf("测试在业务处理的回调里关闭连接,看能不能让客户端停止并重连\n")
		request.GetConnection().Close()
	})
	return nil
}

type Action struct{}

func (a *Action) DialContext(ctx context.Context) (connection.Conn, any, error) {
	var d net.Dialer
	// 连接到本地2000端口的TCP服务器
	conn, err := d.DialContext(ctx, "tcp", "127.0.0.1:2000")
	if err != nil {
		log.Printf("dial失败: %v\n", err)
		return nil, nil, fmt.Errorf("连接失败: %w", err)
	}

	// 可选：配置TCP连接属性（如心跳机制）
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		// 启用TCP保活机制
		if err := tcpConn.SetKeepAlive(true); err != nil {
			log.Printf("设置TCP保活失败: %v\n", err)
		}
	}
	///进行一些握手之类的操作
	return conn, "token", nil
}

// 拨号错误回调 - 返回新的拨号函数用于重连
func (a *Action) DialErr(ctx context.Context, err error) {
	log.Printf("拨号错误: %v, 准备重连...\n", err)
}

func (a *Action) ConnErr(ctx context.Context, conn *connection.Connection, err error) {
	log.Printf("连接断开: %v, 连接信息: %v, 准备重连...\n", err, conn)
}

func (a *Action) ConnectedBegin(ctx context.Context, conn *connection.Connection) {
	log.Printf("成功连接到服务器: %v\n", conn)
	log.Printf("当前Goroutine数量: %d\n", runtime.NumGoroutine())

	// 发送消息 - MsgID=1, 内容="hello from client"
	if err := conn.SendMsg(1, []byte("hello from client")); err != nil {
		log.Printf("发送消息失败: %v\n", err)
		return
	}
	log.Printf("消息已发送 - MsgID: %d, 内容: %s\n", 1, "hello from client")
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
	ctx, cancel := context.WithCancel(context.Background())
	c.Start(ctx) // 启动客户端, 连接断开会自动拨号，直到上下文取消

	time.Sleep(time.Second * 1)
	// 在运行时也可以临时添加消息处理器，MsgID=2对应Handler2
	c.AddHandler(2, &Handler2{})

	// 3秒后移除Handler2, 测试是否真的不会收到移除后的MsgID=2的消息
	go func() {
		time.Sleep(time.Second * 3)
		log.Println("取消 MsgID=2 的 Handler2")
		c.RemoveHandler(2)
		HaveRemoveHandler2 = true
	}()

	go func() {
		time.Sleep(time.Second * 6)
		log.Println("测试停止客户端: 取消客户端上下文, 客户端将停止连接")
		cancel()
		time.Sleep(time.Second)
		if !c.IsStoped() {
			panic("客户端未停止")
		} else {
			log.Println("客户端已停止")
		}
	}()

	// 确保程序退出时正确停止客户端
	defer func() {
		log.Println("正在关闭客户端...")
		// 等待客户端完全停止
		log.Printf("当前Goroutine数量: %d\n", runtime.NumGoroutine())
		<-c.Stop()
		log.Println("客户端已完全停止")
		log.Printf("当前Goroutine数量: %d\n", runtime.NumGoroutine())
	}()

	// 设置信号监听，处理程序退出
	signalChan := make(chan os.Signal, 1)
	signal.Notify(
		signalChan,
		syscall.SIGINT,  // Ctrl+C中断
		syscall.SIGTERM, // 终止信号
		//os.Kill,       // 强制终止
	)

	log.Println("客户端已启动，正在连接到服务器 127.0.0.1:2000...")
	log.Println("按Ctrl+C停止客户端")

	// 等待退出信号
	<-signalChan
	log.Println("收到退出信号，正在停止客户端...")
}
