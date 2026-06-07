# simple-message

[![Go Reference](https://pkg.go.dev/badge/github.com/jursonmo/simple-message.svg)](https://pkg.go.dev/github.com/jursonmo/simple-message)
[![Go Report Card](https://goreportcard.com/badge/github.com/jursonmo/simple-message)](https://goreportcard.com/report/github.com/jursonmo/simple-message)
[![Go Version](https://img.shields.io/badge/go-1.23.4%2B-00ADD8.svg)](https://go.dev/)

simple-message 是一个轻量级 Go 消息通信框架。它把网络连接、消息编解码、消息分发、生命周期管理和客户端重连封装成一组小而清晰的接口，适合用来构建 TCP、WebSocket 或其他基于流式读写连接的消息应用。

> Forked from [s84662355/simple-message](https://github.com/s84662355/simple-message). 本仓库在原项目基础上继续完善运行期 handler 管理、消息处理统计、基于 `context.Context` 的启动/停止流程，以及更稳健的客户端重连节奏。

## 特性

- **轻量协议**：8 字节固定头部，包含 `MsgID` 和数据长度，默认最大消息体为 2MB。
- **统一处理接口**：服务端和客户端共用 `connection.Handler` / `connection.IRequest`。
- **传输层可替换**：只要实现 `connection.Conn` 和 `server.Listener`，就可以接入 TCP、WebSocket 或自定义连接。
- **Context 生命周期**：`Server.Start(ctx, acceptAmount)` 和 `Client.Start(ctx)` 都跟随上层 context 退出。
- **客户端自动重连**：连接断开后自动重新拨号，并保证拨号间隔至少 1 秒，避免异常场景下忙等。
- **运行期 handler 管理**：客户端支持 `AddHandler` / `RemoveHandler` 动态增删消息处理器。
- **连接级状态**：`Connection` 内置属性存储，可保存鉴权信息、会话状态或业务上下文。
- **同步发送确认**：`SendMsg` / `SendMsgContext` 会等待内部发送队列写入完成或返回错误。

## 安装

要求 Go `1.23.4` 或以上版本。

```bash
go get github.com/jursonmo/simple-message
```

## 快速开始

### 服务端

```go
package main

import (
	"context"
	"log"
	"net"
	"os/signal"
	"syscall"

	"github.com/jursonmo/simple-message/connection"
	"github.com/jursonmo/simple-message/server"
)

type EchoHandler struct{}

func (h *EchoHandler) Handle(req connection.IRequest) error {
	log.Printf("recv msg=%d data=%q", req.GetMsgID(), string(req.GetData()))
	return req.GetConnection().SendMsg(req.GetMsgID(), req.GetData())
}

type TCPListener struct {
	net.Listener
}

func (l *TCPListener) Accept() (connection.Conn, any, error) {
	conn, err := l.Listener.Accept()
	return conn, nil, err
}

type ServerAction struct{}

func (a *ServerAction) ConnectedBegin(ctx context.Context, conn *connection.Connection) {
	log.Printf("connected: %s", conn)
}

func (a *ServerAction) ConnErr(ctx context.Context, conn *connection.Connection, err error) {
	log.Printf("connection closed: %v", err)
}

func main() {
	listener, err := net.Listen("tcp", ":2000")
	if err != nil {
		log.Fatal(err)
	}

	srv := server.NewServer(
		&TCPListener{Listener: listener},
		&ServerAction{},
		server.WithHandlers(map[uint32]connection.Handler{
			1: &EchoHandler{},
		}),
		server.WithMaxConnCount(1024),
		server.WithMaxDataLen(1024*1024),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	done := srv.Start(ctx, 4)
	log.Println("server listening on :2000")

	<-ctx.Done()
	srv.Stop()
	<-done
}
```

### 客户端

```go
package main

import (
	"context"
	"log"
	"net"
	"os/signal"
	"syscall"

	"github.com/jursonmo/simple-message/client"
	"github.com/jursonmo/simple-message/connection"
)

type PrintHandler struct{}

func (h *PrintHandler) Handle(req connection.IRequest) error {
	log.Printf("recv msg=%d data=%q", req.GetMsgID(), string(req.GetData()))
	return nil
}

type ClientAction struct {
	addr string
}

func (a *ClientAction) DialContext(ctx context.Context) (connection.Conn, any, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", a.addr)
	return conn, nil, err
}

func (a *ClientAction) ConnectedBegin(ctx context.Context, conn *connection.Connection) {
	log.Printf("connected: %s", conn)
	_ = conn.SendMsg(1, []byte("hello from client"))
}

func (a *ClientAction) ConnErr(ctx context.Context, conn *connection.Connection, err error) {
	log.Printf("connection closed: %v", err)
}

func main() {
	c := client.NewClient(
		&ClientAction{addr: "127.0.0.1:2000"},
		client.WithHandlers(map[uint32]connection.Handler{
			1: &PrintHandler{},
		}),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c.Start(ctx)
	log.Println("client started")

	<-ctx.Done()
	<-c.Stop()
}
```

## 运行示例

TCP 示例：

```bash
go run ./examples/tcp/server
go run ./examples/tcp/client
```

WebSocket 示例：

```bash
go run ./examples/websocket/server
go run ./examples/websocket/client
```

运行测试：

```bash
go test ./...
```

## 协议格式

每条消息由固定头部和变长消息体组成，整数均使用 Big Endian 编码。

| 字段 | 长度 | 说明 |
| --- | --- | --- |
| `MsgID` | 4 bytes | 消息类型 ID |
| `DataLen` | 4 bytes | 消息体长度 |
| `Data` | `DataLen` bytes | 业务数据 |

默认最大消息体为 `protocol.MaxDataLen`，当前是 2MB。可在服务端或客户端初始化时通过 `WithMaxDataLen` 调整。

## 核心 API

### Server

```go
srv := server.NewServer(listener, action, opts...)
done := srv.Start(ctx, acceptAmount)
srv.Stop()
<-done
```

常用配置：

- `server.WithHandlers(map[uint32]connection.Handler)`：注册消息处理器。
- `server.WithMaxDataLen(uint32)`：设置单条消息最大长度。
- `server.WithMaxConnCount(int32)`：限制最大连接数，`0` 表示不限制。

### Client

```go
c := client.NewClient(action, opts...)
c.Start(ctx)
err := c.SendMsg(1, []byte("hello"))
<-c.Stop()
```

常用能力：

- `client.WithHandlers(map[uint32]connection.Handler)`：初始化消息处理器。
- `client.WithMaxDataLen(uint32)`：设置单条消息最大长度。
- `c.AddHandler(msgID, handler)`：运行期添加 handler。
- `c.RemoveHandler(msgID)`：运行期移除 handler。
- `c.SendMsgContext(ctx, msgID, data)`：带超时或取消控制的发送。

### Connection

`connection.Connection` 是 handler 中最常用的对象：

```go
conn := req.GetConnection()
_ = conn.SendMsg(1, []byte("pong"))

conn.StoreProperty("user_id", 1001)
userID, ok := conn.LoadProperty("user_id")
```

## 扩展传输协议

simple-message 不绑定 TCP。自定义传输只需要实现两个接口：

```go
type Conn interface {
	io.ReadWriteCloser
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

type Listener interface {
	Accept() (connection.Conn, any, error)
	Close() error
}
```

仓库内的 `examples/websocket` 展示了如何把 `gorilla/websocket` 包装成 `connection.Conn`，再交给同一套服务端和客户端逻辑处理。

## 项目结构

```text
client/              客户端、自动重连和发送 API
connection/          连接封装、handler 管理、request 抽象
protocol/            消息协议编解码
server/              服务端 accept、连接管理和消息分发
stats/               消息处理统计
examples/tcp/        TCP 示例
examples/websocket/  WebSocket 示例
pkg/taskgo/          内部任务管理工具
```

## 设计取舍

- 这个项目更关注“少量代码快速搭建消息通信”，不是完整 RPC 框架。
- 协议只定义消息边界和 `MsgID`，序列化格式由业务自行选择，例如 JSON、Protobuf 或 MessagePack。
- handler 当前按连接读取循环同步调用；如果业务处理较慢，建议在 handler 内将耗时任务投递到自己的 worker 或队列。
- 客户端会持续重连直到 context 取消，适合长期在线的连接模型。

## 许可证

当前仓库尚未提交 `LICENSE` 文件。对外发布前建议补充明确的开源许可证文本，并让 README 中的许可证声明与仓库文件保持一致。
