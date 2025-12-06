# simple-message

simple-message 是一个基于 Go 语言开发的轻量级消息通信框架，专注于提供简洁、高效的客户端与服务器端通信能力，支持自定义消息处理和连接管理，适用于构建各类基于消息的网络应用。

> 🔀 **Forked from [s84662355/simple-message](https://github.com/s84662355/simple-message)** - simple-message look like inspired by zinx , but more lightweight and concise(更加轻量简洁), 我打算在它的基础上完善下: 1. 可以动态添加handler. 2. 添加每种消息的统计。3. Start(context) 可以关联上层业务，上层业务退出自动退出, 不需要手动Stop(), Stop()至少简单调用ctx cancel. 4. Start启动接口更加规范化. 5. fix: dial() 至少间隔一秒, 避免频繁调用dial()

## 📦 Installation

```bash
go get github.com/jursonmo/simple-message
```
## 核心设计

### 协议规范
框架采用固定格式的消息协议，结构如下：
- **消息头部**：4字节（uint32，大端序），存储消息ID
- **数据长度**：4字节（uint32，大端序），标识数据部分的长度
- **数据内容**：变长字节，实际业务数据

协议默认限制最大数据长度为8KB，可通过初始化参数自定义调整（最大支持1MB及以上，根据业务需求配置）。

### 核心组件

1. **协议编解码器（Decoder）**
   - 负责消息的序列化（Marshal）与反序列化（Unmarshal）
   - 校验数据长度，防止超出配置的最大限制
   - 处理网络IO读写操作，确保数据完整性

2. **连接管理（Connection）**
   - 封装底层网络连接，提供消息发送接口（`SendMsg`/`SendMsgContext`）
   - 支持上下文管理（Context），便于资源控制和超时处理
   - 内置连接属性存储（基于`sync.Map`），可存储会话信息等自定义数据
   - 提供连接关闭和状态管理机制

3. **消息处理机制**
   - 基于消息ID映射处理器（`Handler`），支持不同类型消息的差异化处理
   - 消息处理器接口（`IRequest`）提供消息ID、数据和连接的访问能力
   - 异步消息处理队列，通过`MessageBody`实现消息发送确认（Ack）机制

4. **服务器（Server）**
   - 支持多协程Accept连接（可配置数量），提高并发处理能力
   - 连接数量控制，防止资源耗尽
   - 连接事件回调（建立/错误），便于业务层处理
   - 自定义监听器接口，可扩展支持不同传输协议（默认提供TCP实现）

5. **客户端（Client）**
   - 自动重连机制，网络异常时尝试重新建立连接
   - 连接状态管理，提供连接建立/错误回调
   - 与服务器端统一的消息处理接口，开发体验一致

## 安装说明

```bash
# 要求Go 1.23.4及以上版本
go get github.com/jursonmo/simple-message
```

## 快速上手

### 服务器端实现步骤

1. **定义消息处理器**
```go
// 处理MsgID=1的消息
type Handler1 struct{}

func (h *Handler1) Handle(request connection.IRequest) error {
    fmt.Printf("收到消息 - ID: %d, 内容: %s\n",
        request.GetMsgID(),
        string(request.GetData()))
    // 业务处理逻辑...
    return nil
}
```

2. **实现监听器（以TCP为例）**
```go
type TCPListener struct {
    listener net.Listener
}

func (l *TCPListener) Accept() (connection.Conn, any, error) {
    conn, err := l.listener.Accept()
    if err != nil {
        return nil, nil, err
    }
    // 可在此处进行握手、认证等操作
    return conn, "initial data", nil
}

func (l *TCPListener) Close() error {
    return l.listener.Close()
}
```

3. **实现连接事件回调**
```go
type ServerAction struct{}

func (a *ServerAction) ConnErr(ctx context.Context, conn *connection.Connection, err error) {
    fmt.Printf("连接错误: %v\n", err)
}

func (a *ServerAction) ConnectedBegin(ctx context.Context, conn *connection.Connection) {
    fmt.Println("新连接建立")
    // 发送欢迎消息
    if err := conn.SendMsg(1, []byte("欢迎连接到服务器")); err != nil {
        fmt.Printf("发送消息失败: %v\n", err)
    }
}
```

4. **启动服务器**
```go
func main() {
    // 创建TCP监听器
    listener, _ := net.Listen("tcp", ":2000")
    defer listener.Close()

    // 注册消息处理器
    handlers := map[uint32]connection.Handler{
        1: &Handler1{},
    }

    // 创建服务器实例
    srv := server.NewServer(
        &TCPListener{listener: listener},
        &ServerAction{},
        server.WithHandlers(handlers),
        server.WithMaxDataLen(1024*1024), // 最大数据长度
        server.WithMaxConnCount(1024),      // 最大连接数
    )

    	// 启动服务器，使用16个accept协程
	ctx, cancel := context.WithCancel(context.Background())
	_ = cancel //取消ctx时，可以关闭server
	done := srv.Start(ctx, 16)
    done := srv.Start(16)
    defer func() {
        srv.Stop()
        <-done
    }()

    // 等待退出信号...
}
```

### 客户端实现步骤

1. **定义消息处理器（与服务器端类似）**
```go
type ClientHandler struct{}

func (h *ClientHandler) Handle(request connection.IRequest) error {
    fmt.Printf("收到消息 - ID: %d, 内容: %s\n",
        request.GetMsgID(),
        string(request.GetData()))
    return nil
}
```

2. **实现客户端动作接口**
```go
type ClientAction struct{}

func (a *ClientAction) DialContext(ctx context.Context) (connection.Conn, any, error) {
    var d net.Dialer
    conn, err := d.DialContext(ctx, "tcp", "127.0.0.1:2000")
    if err != nil {
        return nil, nil, err
    }
    // 配置TCP参数（如保活）
    if tcpConn, ok := conn.(*net.TCPConn); ok {
        _ = tcpConn.SetKeepAlive(true)
    }
    return conn, "client data", nil
}

func (a *ClientAction) ConnErr(ctx context.Context, conn *connection.Connection, err error) {
    fmt.Printf("连接错误: %v, 准备重连...\n", err)
}

func (a *ClientAction) ConnectedBegin(ctx context.Context, conn *connection.Connection) {
    fmt.Println("成功连接到服务器")
}
```

3. **启动客户端**
```go
func main() {
    // 注册消息处理器
    handlers := map[uint32]connection.Handler{
        1: &ClientHandler{},
    }

    // 创建客户端实例
    c := client.NewClient(
        &ClientAction{},
        client.WithHandlers(handlers),
        client.WithMaxDataLen(1024*1024), // 最大数据长度
    )

    // 启动客户端
	ctx, cancel := context.WithCancel(context.Background())
	_ = cancel // 忽略取消函数, 因为我们不会主动取消上下文

	c.Start(ctx) // 启动客户端, 连接断开会自动拨号，直到上下文取消

    defer func() {
        <-c.Stop()
    }()

   	// 设置信号监听，处理程序退出
	signalChan := make(chan os.Signal, 1)
	signal.Notify(
		signalChan,
		syscall.SIGINT,  // Ctrl+C中断
		syscall.SIGTERM, // 终止信号
	)
    // 等待退出信号
	<-signalChan
}
```

## 高级特性

1. **连接属性管理**
   连接对象提供丰富的属性操作方法，可用于存储会话信息：
   ```go
   // 存储属性
   conn.StoreProperty("user_id", 123)
   
   // 获取属性
   if val, ok := conn.LoadProperty("user_id"); ok {
       // 使用属性值...
   }
   ```

2. **上下文控制**
   支持基于Context的超时控制和资源释放：
   ```go
   // 带超时的消息发送
   ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
   defer cancel()
   err := conn.SendMsgContext(ctx, 1, []byte("timeout test"))
   ```

3. **自定义协议扩展**
   通过实现自定义的`Decoder`和`Listener`，可支持非TCP协议或自定义消息格式。

## 许可证

本项目采用MIT许可证开源，详情参见[LICENSE](LICENSE)文件。