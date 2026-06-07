package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jursonmo/simple-message/connection"
	"github.com/jursonmo/simple-message/protocol"
)

// blockingAction 模拟一个会一直阻塞到 ctx 取消的拨号动作。
// 这类 action 适合测试 Start/Stop 生命周期，因为它不会真的建立连接，也不会进入 HandlerManager。
type blockingAction struct {
	dialCount atomic.Int32
	once      sync.Once
	dialed    chan struct{}
}

func newBlockingAction() *blockingAction {
	return &blockingAction{
		dialed: make(chan struct{}),
	}
}

func (a *blockingAction) DialContext(ctx context.Context) (connection.Conn, any, error) {
	a.dialCount.Add(1)
	a.once.Do(func() {
		close(a.dialed)
	})
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

func (a *blockingAction) ConnErr(ctx context.Context, conn *connection.Connection, err error) {}

func (a *blockingAction) ConnectedBegin(ctx context.Context, conn *connection.Connection) {}

type clientTestAddr string

func (a clientTestAddr) Network() string { return "test" }

func (a clientTestAddr) String() string { return string(a) }

// clientTestConn 是最小可用连接，主要用于直接构造 connection.Connection。
// 这些测试只验证 Client 的发送封装，不需要真实读写底层网络。
type clientTestConn struct{}

func (clientTestConn) Read(p []byte) (int, error) { return 0, io.EOF }

func (clientTestConn) Write(p []byte) (int, error) { return len(p), nil }

func (clientTestConn) Close() error { return nil }

func (clientTestConn) LocalAddr() net.Addr { return clientTestAddr("local") }

func (clientTestConn) RemoteAddr() net.Addr { return clientTestAddr("remote") }

// scriptedConn 用内存字节流和 channel 模拟可控的底层连接。
// readData 会先被 Read 读完；读完后 Read 阻塞到 Close，用来稳定测试 dialAndRun。
type scriptedConn struct {
	mu          sync.Mutex
	reader      *bytes.Reader
	closed      chan struct{}
	readStarted chan struct{}
	writes      chan []byte
	once        sync.Once
}

func newScriptedConn(readData []byte) *scriptedConn {
	return &scriptedConn{
		reader:      bytes.NewReader(readData),
		closed:      make(chan struct{}),
		readStarted: make(chan struct{}, 1),
		writes:      make(chan []byte, 8),
	}
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	select {
	case c.readStarted <- struct{}{}:
	default:
	}

	c.mu.Lock()
	if c.reader.Len() > 0 {
		n, err := c.reader.Read(p)
		c.mu.Unlock()
		return n, err
	}
	c.mu.Unlock()

	<-c.closed
	return 0, io.ErrClosedPipe
}

func (c *scriptedConn) Write(p []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, io.ErrClosedPipe
	default:
	}

	// 复制写入内容，避免调用方复用底层切片后影响断言。
	cp := append([]byte(nil), p...)
	c.writes <- cp
	return len(p), nil
}

func (c *scriptedConn) Close() error {
	c.once.Do(func() {
		close(c.closed)
	})
	return nil
}

func (c *scriptedConn) LocalAddr() net.Addr { return clientTestAddr("local") }

func (c *scriptedConn) RemoteAddr() net.Addr { return clientTestAddr("remote") }

type clientConnErr struct {
	conn *connection.Connection
	err  error
}

// recordingClientAction 记录 Client 的生命周期回调，便于断言拨号、连接建立和连接错误。
type recordingClientAction struct {
	conn      connection.Conn
	data      any
	err       error
	dialCount atomic.Int32
	once      sync.Once
	dialed    chan struct{}
	connected chan *connection.Connection
	connErrs  chan clientConnErr
}

func newRecordingClientAction(conn connection.Conn, data any) *recordingClientAction {
	return &recordingClientAction{
		conn:      conn,
		data:      data,
		dialed:    make(chan struct{}),
		connected: make(chan *connection.Connection, 8),
		connErrs:  make(chan clientConnErr, 8),
	}
}

func (a *recordingClientAction) DialContext(ctx context.Context) (connection.Conn, any, error) {
	a.dialCount.Add(1)
	a.once.Do(func() {
		close(a.dialed)
	})
	if a.err != nil {
		return nil, nil, a.err
	}
	return a.conn, a.data, nil
}

func (a *recordingClientAction) ConnErr(ctx context.Context, conn *connection.Connection, err error) {
	a.connErrs <- clientConnErr{conn: conn, err: err}
}

func (a *recordingClientAction) ConnectedBegin(ctx context.Context, conn *connection.Connection) {
	a.connected <- conn
}

// recordingHandler 记录 handler 收到的 request，并可按需返回错误。
// 测试 handleMsg 分发和统计时，用它验证 request 内容与成功/失败计数。
type recordingHandler struct {
	err      error
	calls    atomic.Int32
	requests chan connection.IRequest
}

func newRecordingHandler(err error) *recordingHandler {
	return &recordingHandler{
		err:      err,
		requests: make(chan connection.IRequest, 8),
	}
}

func (h *recordingHandler) Handle(req connection.IRequest) error {
	h.calls.Add(1)
	h.requests <- req
	return h.err
}

// 测试目的：已注册 MsgID 时，client 应把消息分发给对应 handler，并记录成功统计。
// 测试方法：直接调用 handleMsg，检查 handler 收到的 request 内容和 Success 统计。
func TestClientHandleMsgDispatchesAndRecordsSuccessStats(t *testing.T) {
	// 准备一个已注册的 handler，用于接收 MsgID=7 的消息。
	handler := newRecordingHandler(nil)
	c := NewClient(
		nil,
		WithHandlers(map[uint32]connection.Handler{7: handler}),
	)
	conn := connection.NewConnection(clientTestConn{}, "conn-data")

	// 直接调用 handleMsg，模拟 HandlerManager 收到并分发了一条消息。
	c.handleMsg(conn, &protocol.Message{
		MsgID: 7,
		Data:  []byte("payload"),
	})

	// 验证 handler 收到的 request 与原始消息一致。
	req := receiveRequest(t, handler)
	if req.GetConnection() != conn {
		t.Fatal("handler received unexpected connection")
	}
	if req.GetMsgID() != 7 {
		t.Fatalf("request MsgID = %d, want 7", req.GetMsgID())
	}
	if !bytes.Equal(req.GetData(), []byte("payload")) {
		t.Fatalf("request data = %q, want %q", req.GetData(), "payload")
	}

	// 验证成功统计被正确累加，失败统计保持为 0。
	stat := c.stats.GetStatistic(7)
	if stat.SuccessPacket != 1 {
		t.Fatalf("SuccessPacket = %d, want 1", stat.SuccessPacket)
	}
	if stat.SuccessBytes != uint64(len("payload")) {
		t.Fatalf("SuccessBytes = %d, want %d", stat.SuccessBytes, len("payload"))
	}
	if stat.FailedPacket != 0 || stat.FailedBytes != 0 {
		t.Fatalf("failed stats = packets:%d bytes:%d, want zero", stat.FailedPacket, stat.FailedBytes)
	}
}

// 测试目的：handler 返回错误时，client 应记录失败包数和失败字节数。
// 测试方法：让 recordingHandler 返回固定错误，然后检查 Failed 统计且 Success 为 0。
func TestClientHandleMsgRecordsFailedStats(t *testing.T) {
	wantErr := errors.New("handler failed")
	// 准备一个会返回错误的 handler，用于触发失败统计。
	handler := newRecordingHandler(wantErr)
	c := NewClient(
		nil,
		WithHandlers(map[uint32]connection.Handler{8: handler}),
	)

	// 分发一条 MsgID=8 的消息，handler 返回错误后应计为失败。
	c.handleMsg(
		connection.NewConnection(clientTestConn{}, nil),
		&protocol.Message{MsgID: 8, Data: []byte("bad")},
	)

	// 等待 handler 被调用，避免只检查统计而漏掉分发失败。
	_ = receiveRequest(t, handler)
	stat := c.stats.GetStatistic(8)
	if stat.FailedPacket != 1 {
		t.Fatalf("FailedPacket = %d, want 1", stat.FailedPacket)
	}
	if stat.FailedBytes != uint64(len("bad")) {
		t.Fatalf("FailedBytes = %d, want %d", stat.FailedBytes, len("bad"))
	}
	if stat.SuccessPacket != 0 || stat.SuccessBytes != 0 {
		t.Fatalf("success stats = packets:%d bytes:%d, want zero", stat.SuccessPacket, stat.SuccessBytes)
	}
}

// 测试目的：没有注册 handler 的 MsgID 应被计入 UnknownMsg。
// 测试方法：发送不存在的 MsgID，检查 UnknownMsg 增加且该 MsgID 没有成功/失败统计。
func TestClientHandleMsgUnknownIncrementsStats(t *testing.T) {
	c := NewClient(nil)

	// 没有注册 MsgID=404，handleMsg 应只记录 unknown，不调用任何 handler。
	c.handleMsg(
		connection.NewConnection(clientTestConn{}, nil),
		&protocol.Message{MsgID: 404, Data: []byte("missing")},
	)

	// 验证 unknown 计数增加，并且不会创建成功/失败包统计。
	if got := c.stats.GetUnknownMsg(); got != 1 {
		t.Fatalf("UnknownMsg = %d, want 1", got)
	}
	stat := c.stats.GetStatistic(404)
	if stat.SuccessPacket != 0 || stat.FailedPacket != 0 {
		t.Fatalf("unexpected stats for unknown message: %+v", stat)
	}
}

// 测试目的：WithHandlers 应复制传入的 map，避免 NewClient 后外部修改影响 client。
// 测试方法：创建 client 后修改原 map，再验证旧 handler 仍生效、新增 handler 不生效。
func TestClientWithHandlersClonesMap(t *testing.T) {
	// 先用 handlers 创建 client，后续会修改原 map 来验证是否被隔离。
	first := newRecordingHandler(nil)
	second := newRecordingHandler(nil)
	handlers := map[uint32]connection.Handler{1: first}
	c := NewClient(nil, WithHandlers(handlers))

	// 修改原始 map：替换旧 handler，并额外添加新的 MsgID。
	handlers[1] = second
	handlers[2] = second

	// MsgID=1 仍应命中创建 client 时复制进去的 first handler。
	c.handleMsg(
		connection.NewConnection(clientTestConn{}, nil),
		&protocol.Message{MsgID: 1, Data: []byte("one")},
	)
	_ = receiveRequest(t, first)
	if got := second.calls.Load(); got != 0 {
		t.Fatalf("mutated handler was called %d times, want 0", got)
	}

	// MsgID=2 是创建后才加到原 map 的，应被 client 当作未知消息处理。
	c.handleMsg(
		connection.NewConnection(clientTestConn{}, nil),
		&protocol.Message{MsgID: 2, Data: []byte("two")},
	)
	if got := c.stats.GetUnknownMsg(); got != 1 {
		t.Fatalf("UnknownMsg = %d, want 1", got)
	}
	if got := second.calls.Load(); got != 0 {
		t.Fatalf("handler added after NewClient was called %d times, want 0", got)
	}
}

// 测试目的：AddHandler 和 RemoveHandler 应动态增删消息处理器。
// 测试方法：先新增 handler 并确认能收到消息，再移除同一个 MsgID 并确认后续消息变为 unknown。
func TestClientAddAndRemoveHandler(t *testing.T) {
	handler := newRecordingHandler(nil)
	c := NewClient(nil)

	// 动态添加 handler 后，MsgID=3 应能正常分发。
	c.AddHandler(3, handler)
	c.handleMsg(
		connection.NewConnection(clientTestConn{}, nil),
		&protocol.Message{MsgID: 3, Data: []byte("first")},
	)
	_ = receiveRequest(t, handler)

	// 移除同一个 MsgID 后，再收到消息应进入 unknown 统计。
	c.RemoveHandler(3)
	c.handleMsg(
		connection.NewConnection(clientTestConn{}, nil),
		&protocol.Message{MsgID: 3, Data: []byte("second")},
	)

	if got := handler.calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
	if got := c.stats.GetUnknownMsg(); got != 1 {
		t.Fatalf("UnknownMsg = %d, want 1", got)
	}
}

// 测试目的：WithMaxDataLen 应把自定义消息体上限保存到 client。
// 测试方法：创建 client 后直接检查包内字段，确保后续 HandlerManager 会使用该上限。
func TestClientWithMaxDataLenSetsLimit(t *testing.T) {
	// 设置较小的消息体上限，验证 option 生效。
	c := NewClient(nil, WithMaxDataLen(4))

	if c.maxDataLen != 4 {
		t.Fatalf("maxDataLen = %d, want 4", c.maxDataLen)
	}
}

// 测试目的：还未建立连接时，SendMsg 应返回 ErrConn。
// 测试方法：不启动 client、不设置 connPointer，直接调用 SendMsg。
func TestClientSendMsgBeforeConnectedReturnsErrConn(t *testing.T) {
	// 不设置 connPointer，模拟 client 还没有成功建立连接。
	c := NewClient(nil)

	// 未连接时发送消息，应返回 client 层的连接错误。
	if err := c.SendMsg(1, []byte("hello")); !errors.Is(err, ErrConn) {
		t.Fatalf("SendMsg error = %v, want %v", err, ErrConn)
	}
}

// 测试目的：SendMsgContext 应通过当前连接发送消息，并把 ack 结果返回给调用方。
// 测试方法：手动设置 connPointer，读取连接队列里的 MessageBody，校验内容后 ack 成功。
func TestClientSendMsgContextUsesCurrentConnection(t *testing.T) {
	// 手动放入当前连接，绕开真实拨号流程，专注测试 SendMsgContext 的转发逻辑。
	c := NewClient(nil)
	conn := connection.NewConnection(clientTestConn{}, "current")
	c.connPointer.Store(conn)
	errCh := make(chan error, 1)

	// SendMsgContext 会等待底层发送任务 ack，因此放到 goroutine 中执行。
	go func() {
		errCh <- c.SendMsgContext(context.Background(), 11, []byte("hello"))
	}()

	// 从连接的发送队列中取出消息，验证 MsgID 和数据内容。
	msg := receiveQueuedMessage(t, conn)
	if got := msg.GetMessage().MsgID; got != 11 {
		t.Fatalf("queued MsgID = %d, want 11", got)
	}
	if !bytes.Equal(msg.GetMessage().Data, []byte("hello")) {
		t.Fatalf("queued data = %q, want %q", msg.GetMessage().Data, "hello")
	}

	// AckMessage 模拟底层发送任务已完成写入，SendMsgContext 应随即返回 nil。
	msg.AckMessage(func() error {
		return nil
	})
	if err := receiveSendErr(t, errCh); err != nil {
		t.Fatalf("SendMsgContext error = %v, want nil", err)
	}
}

// 测试目的：底层发送任务 ack 错误时，SendMsgContext 应把错误原样返回。
// 测试方法：读取队列消息后用自定义错误 ack，检查调用方收到同一个错误。
func TestClientSendMsgContextReturnsAckError(t *testing.T) {
	// 准备一个当前连接，并设置底层写入将要返回的错误。
	c := NewClient(nil)
	conn := connection.NewConnection(clientTestConn{}, nil)
	c.connPointer.Store(conn)
	wantErr := errors.New("write failed")
	errCh := make(chan error, 1)

	// 发送调用会阻塞到消息被 ack。
	go func() {
		errCh <- c.SendMsgContext(context.Background(), 12, []byte("bad"))
	}()

	// 用自定义错误完成 ack，模拟底层编码/写入失败。
	msg := receiveQueuedMessage(t, conn)
	msg.AckMessage(func() error {
		return wantErr
	})

	// 调用方应收到 ack 中返回的同一个错误。
	if err := receiveSendErr(t, errCh); !errors.Is(err, wantErr) {
		t.Fatalf("SendMsgContext error = %v, want %v", err, wantErr)
	}
}

// 测试目的：消息入队后，如果调用方 context 被取消，SendMsgContext 应及时返回 context 错误。
// 测试方法：先让消息入队但不 ack，再取消 context，确认不会永久阻塞。
func TestClientSendMsgContextReturnsWhenContextCancelsAfterQueue(t *testing.T) {
	// 准备可取消 context，用来验证消息入队后的取消路径。
	c := NewClient(nil)
	conn := connection.NewConnection(clientTestConn{}, nil)
	c.connPointer.Store(conn)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- c.SendMsgContext(ctx, 13, []byte("queued"))
	}()

	// 确认消息已经入队但不 ack，此时 SendMsgContext 会继续等待。
	_ = receiveQueuedMessage(t, conn)
	cancel()

	// context 取消后，发送调用应立即带 context.Canceled 返回。
	if err := receiveSendErr(t, errCh); !errors.Is(err, context.Canceled) {
		t.Fatalf("SendMsgContext error = %v, want %v", err, context.Canceled)
	}
}

// 测试目的：dialAndRun 建立连接后，应通过 HandlerManager 读取协议消息并分发给 client handler。
// 测试方法：用内存协议字节模拟服务端消息，等待 handler 收到 request，再检查成功统计。
func TestClientDialAndRunReceivesInboundMessage(t *testing.T) {
	// 先把一条协议消息编码成字节流，作为 scriptedConn 的读数据。
	var inbound bytes.Buffer
	if err := protocol.NewDecoder(16).Marshal(&inbound, 21, []byte("from-server")); err != nil {
		t.Fatalf("marshal inbound message failed: %v", err)
	}

	// 启动 dialAndRun，让 HandlerManager 从内存连接读取并分发消息。
	handler := newRecordingHandler(nil)
	rawConn := newScriptedConn(inbound.Bytes())
	action := newRecordingClientAction(rawConn, "dial-data")
	c := NewClient(
		action,
		WithMaxDataLen(16),
		WithHandlers(map[uint32]connection.Handler{21: handler}),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := runDialAndRun(c, ctx)

	// 等待 handler 收到服务端消息，并检查解码后的 request 内容。
	req := receiveRequest(t, handler)
	if req.GetMsgID() != 21 {
		t.Fatalf("request MsgID = %d, want 21", req.GetMsgID())
	}
	if !bytes.Equal(req.GetData(), []byte("from-server")) {
		t.Fatalf("request data = %q, want %q", req.GetData(), "from-server")
	}
	if stat := c.stats.GetStatistic(21); stat.SuccessPacket != 1 {
		t.Fatalf("SuccessPacket = %d, want 1", stat.SuccessPacket)
	}

	// 停止 dialAndRun，避免测试结束后留下 goroutine。
	stopDialAndRun(t, cancel, rawConn, done)
}

// 测试目的：dialAndRun 建立连接后，SendMsg 应经过 HandlerManager 编码并写入底层连接。
// 测试方法：启动 dialAndRun，发送一条消息，解析 scriptedConn 记录的写入字节并校验协议内容。
func TestClientDialAndRunSendsEncodedMessage(t *testing.T) {
	// scriptedConn 没有预置读数据，用来重点观察发送方向的写入内容。
	rawConn := newScriptedConn(nil)
	action := newRecordingClientAction(rawConn, "dial-data")
	c := NewClient(action, WithMaxDataLen(32))
	ctx, cancel := context.WithCancel(context.Background())
	done := runDialAndRun(c, ctx)

	// 等待 ConnectedBegin 回调，确认 HandlerManager 已经接管连接。
	connected := receiveConnected(t, action)
	if connected.GetData() != "dial-data" {
		t.Fatalf("connection data = %v, want %v", connected.GetData(), "dial-data")
	}
	waitForClientConnection(t, c)

	// 通过 client 发送消息，底层应写入协议编码后的完整字节。
	if err := c.SendMsg(31, []byte("from-client")); err != nil {
		t.Fatalf("SendMsg error = %v, want nil", err)
	}

	// 把写入的字节重新按协议解码，验证 MsgID 和 payload。
	written := receiveWritten(t, rawConn)
	msg, err := protocol.NewDecoder(32).Unmarshal(bytes.NewReader(written))
	if err != nil {
		t.Fatalf("unmarshal written message failed: %v", err)
	}
	if msg.MsgID != 31 {
		t.Fatalf("written MsgID = %d, want 31", msg.MsgID)
	}
	if !bytes.Equal(msg.Data, []byte("from-client")) {
		t.Fatalf("written data = %q, want %q", msg.Data, "from-client")
	}

	// 停止连接后，ConnErr 应拿到与 ConnectedBegin 相同的 connection。
	stopDialAndRun(t, cancel, rawConn, done)
	if got := receiveClientConnErr(t, action); got.conn != connected {
		t.Fatal("ConnErr received unexpected connection")
	}
}

// 测试目的：重复调用 Start 不应重复拨号。
// 测试方法：第一次 Start 后再调用 Start，确认 DialContext 只被调用 1 次。
func TestClientStartIsIdempotent(t *testing.T) {
	action := newBlockingAction()
	c := NewClient(action)

	// 连续调用 Start，只有第一次应真正启动拨号 goroutine。
	c.Start(context.Background())
	c.Start(context.Background())

	// 等待拨号开始后，确认拨号次数不会继续增加。
	waitForDial(t, action)
	assertDialCountStays(t, action, 1)

	// Stop 取消 client context，并等待 Start 创建的 done 关闭。
	done := c.Stop()
	if done == nil {
		t.Fatal("expected non-nil done channel after Start")
	}
	waitForClientDone(t, done)
	if !c.IsStoped() {
		t.Fatal("client is not marked stopped after Stop")
	}
}

// 测试目的：并发调用 Start 也只能启动一次拨号流程。
// 测试方法：多个 goroutine 同时 Start，等待全部返回后确认 DialContext 只被调用 1 次。
func TestClientConcurrentStartIsIdempotent(t *testing.T) {
	action := newBlockingAction()
	c := NewClient(action)

	// 多个 goroutine 同时调用 Start，用来覆盖 sync.Once 的并发场景。
	const starters = 32
	var wg sync.WaitGroup
	wg.Add(starters)
	for i := 0; i < starters; i++ {
		go func() {
			defer wg.Done()
			c.Start(context.Background())
		}()
	}
	wg.Wait()

	// 不管并发调用多少次，都只能触发一次 DialContext。
	waitForDial(t, action)
	assertDialCountStays(t, action, 1)

	waitForClientDone(t, c.Stop())
}

// 测试目的：父 context 取消时，client 应停止拨号循环并标记为 stopped。
// 测试方法：Start 使用可取消 context，确认 DialContext 已阻塞后取消父 context，再等待 done。
func TestClientStopsWhenParentContextCanceled(t *testing.T) {
	action := newBlockingAction()
	c := NewClient(action)
	ctx, cancel := context.WithCancel(context.Background())

	// 使用父 context 启动 client，并确认拨号已经进入阻塞状态。
	c.Start(ctx)
	waitForDial(t, action)
	cancel()

	// 父 context 取消后，client 应结束并更新 stopped 状态。
	waitForClientDone(t, c.Stop())
	if !c.IsStoped() {
		t.Fatal("client is not marked stopped after parent context was canceled")
	}
}

// 测试目的：client 未 Start 前调用 Stop 不应创建 done channel。
// 测试方法：直接 Stop，期望返回 nil。
func TestClientStopBeforeStartReturnsNil(t *testing.T) {
	// 未 Start 时，client 内部还没有 cancel 和 done。
	c := NewClient(newBlockingAction())

	if done := c.Stop(); done != nil {
		t.Fatal("expected nil done channel before client start")
	}
}

// 测试目的：SleepAtLeast 在 ctx 已取消时应立刻返回，不应继续等待完整 sleep 时间。
// 测试方法：传入已取消 context 和较长等待时间，确认函数很快返回。
func TestSleepAtLeastReturnsWhenContextCanceled(t *testing.T) {
	// 创建已取消的 context，SleepAtLeast 应优先响应取消。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	SleepAtLeast(ctx, start, time.Second)

	// 用较宽松的阈值判断，避免不同机器上的调度抖动导致误判。
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("SleepAtLeast elapsed = %v, want less than 100ms", elapsed)
	}
}

// 测试目的：SleepAtLeast 在已超过最小等待时间时应直接返回。
// 测试方法：把 start 设置到过去，确认函数不会额外等待。
func TestSleepAtLeastReturnsImmediatelyWhenElapsedAlreadyEnough(t *testing.T) {
	start := time.Now()

	// start 设置到过去，表示已经满足最小等待时间。
	SleepAtLeast(context.Background(), start.Add(-time.Second), time.Millisecond)

	// 已满足等待时间时不应再额外阻塞。
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("SleepAtLeast elapsed = %v, want less than 100ms", elapsed)
	}
}

func runDialAndRun(c *Client, ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.dialAndRun(ctx)
	}()
	return done
}

func stopDialAndRun(t *testing.T, cancel context.CancelFunc, conn *scriptedConn, done <-chan struct{}) {
	t.Helper()

	cancel()
	// 先取消 ctx，让 dialAndRun 进入停止流程；再关闭底层连接，解除 Read 阻塞。
	time.Sleep(10 * time.Millisecond)
	if err := conn.Close(); err != nil {
		t.Fatalf("close scripted conn failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("dialAndRun did not stop")
	}
}

func receiveRequest(t *testing.T, handler *recordingHandler) connection.IRequest {
	t.Helper()

	select {
	case req := <-handler.requests:
		return req
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handler was not called")
		return nil
	}
}

func receiveQueuedMessage(t *testing.T, conn *connection.Connection) *connection.MessageBody {
	t.Helper()

	select {
	case msg := <-conn.MsgChan():
		return msg
	case <-time.After(500 * time.Millisecond):
		t.Fatal("message was not queued")
		return nil
	}
}

func receiveSendErr(t *testing.T, errCh <-chan error) error {
	t.Helper()

	select {
	case err := <-errCh:
		return err
	case <-time.After(500 * time.Millisecond):
		t.Fatal("SendMsgContext did not return")
		return nil
	}
}

func receiveConnected(t *testing.T, action *recordingClientAction) *connection.Connection {
	t.Helper()

	select {
	case conn := <-action.connected:
		return conn
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ConnectedBegin callback was not called")
		return nil
	}
}

func receiveClientConnErr(t *testing.T, action *recordingClientAction) clientConnErr {
	t.Helper()

	select {
	case err := <-action.connErrs:
		return err
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ConnErr callback was not called")
		return clientConnErr{}
	}
}

func receiveWritten(t *testing.T, conn *scriptedConn) []byte {
	t.Helper()

	select {
	case written := <-conn.writes:
		return written
	case <-time.After(500 * time.Millisecond):
		t.Fatal("connection did not receive a write")
		return nil
	}
}

func waitForClientConnection(t *testing.T, c *Client) *connection.Connection {
	t.Helper()

	deadline := time.After(500 * time.Millisecond)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		if conn := c.connPointer.Load(); conn != nil {
			return conn
		}

		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatal("client connection pointer was not set")
			return nil
		}
	}
}

func waitForDial(t *testing.T, action *blockingAction) {
	t.Helper()

	select {
	case <-action.dialed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("client did not start dialing")
	}
}

func assertDialCountStays(t *testing.T, action *blockingAction, want int32) {
	t.Helper()

	time.Sleep(50 * time.Millisecond)
	if got := action.dialCount.Load(); got != want {
		t.Fatalf("dial count = %d, want %d", got, want)
	}
}

func waitForClientDone(t *testing.T, done <-chan struct{}) {
	t.Helper()

	if done == nil {
		t.Fatal("client returned nil done channel")
	}
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("client did not stop")
	}
}
