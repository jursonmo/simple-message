package server

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

type testAction struct{}

func (testAction) ConnErr(ctx context.Context, conn *connection.Connection, err error) {}

func (testAction) ConnectedBegin(ctx context.Context, conn *connection.Connection) {}

// recordingAction 用来记录 server 生命周期回调，便于测试连接建立和连接错误是否被触发。
type recordingAction struct {
	connected chan *connection.Connection
	connErrs  chan error
}

func newRecordingAction() *recordingAction {
	return &recordingAction{
		connected: make(chan *connection.Connection, 8),
		connErrs:  make(chan error, 8),
	}
}

func (a *recordingAction) ConnErr(ctx context.Context, conn *connection.Connection, err error) {
	a.connErrs <- err
}

func (a *recordingAction) ConnectedBegin(ctx context.Context, conn *connection.Connection) {
	a.connected <- conn
}

// blockingListener 模拟会一直阻塞在 Accept 的 listener。
// 测试 Start/Stop 行为时，用 Close 解除阻塞，避免依赖真实端口。
type blockingListener struct {
	closed        chan struct{}
	acceptStarted chan struct{}
	once          sync.Once
	acceptCount   atomic.Int32
}

func newBlockingListener() *blockingListener {
	return &blockingListener{
		closed:        make(chan struct{}),
		acceptStarted: make(chan struct{}, 128),
	}
}

func (l *blockingListener) Accept() (connection.Conn, any, error) {
	l.acceptCount.Add(1)
	select {
	case l.acceptStarted <- struct{}{}:
	default:
	}
	<-l.closed
	return nil, nil, net.ErrClosed
}

func (l *blockingListener) Close() error {
	l.once.Do(func() {
		close(l.closed)
	})
	return nil
}

type acceptResult struct {
	conn connection.Conn
	data any
	err  error
}

// queueListener 通过 channel 按测试需要投递连接。
// 测试连接数限制、协议解析错误等场景时，可以精确控制 Accept 返回值。
type queueListener struct {
	accepts chan acceptResult
	closed  chan struct{}
	once    sync.Once
}

func newQueueListener(buffer int) *queueListener {
	return &queueListener{
		accepts: make(chan acceptResult, buffer),
		closed:  make(chan struct{}),
	}
}

func (l *queueListener) Accept() (connection.Conn, any, error) {
	select {
	case r := <-l.accepts:
		return r.conn, r.data, r.err
	case <-l.closed:
		return nil, nil, net.ErrClosed
	}
}

func (l *queueListener) Close() error {
	l.once.Do(func() {
		close(l.closed)
	})
	return nil
}

func (l *queueListener) enqueue(conn connection.Conn, data any) {
	l.accepts <- acceptResult{conn: conn, data: data}
}

type serverTestAddr string

func (a serverTestAddr) Network() string { return "test" }

func (a serverTestAddr) String() string { return string(a) }

// blockingConn 模拟一个读操作会阻塞到 Close 的连接。
// 测试活跃连接和 Stop 关闭连接时，用 readStarted 确认 handler 已经接管连接。
type blockingConn struct {
	closed      chan struct{}
	readStarted chan struct{}
	once        sync.Once
}

func newBlockingConn() *blockingConn {
	return &blockingConn{
		closed:      make(chan struct{}),
		readStarted: make(chan struct{}, 1),
	}
}

func (c *blockingConn) Read(p []byte) (int, error) {
	select {
	case c.readStarted <- struct{}{}:
	default:
	}
	<-c.closed
	return 0, io.ErrClosedPipe
}

func (c *blockingConn) Write(p []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, io.ErrClosedPipe
	default:
		return len(p), nil
	}
}

func (c *blockingConn) Close() error {
	c.once.Do(func() {
		close(c.closed)
	})
	return nil
}

func (c *blockingConn) LocalAddr() net.Addr { return serverTestAddr("local") }

func (c *blockingConn) RemoteAddr() net.Addr { return serverTestAddr("remote") }

// bufferConn 用内存数据模拟客户端发来的字节流。
// 测试协议解码和 maxDataLen 时，不需要启动真实网络连接。
type bufferConn struct {
	reader *bytes.Reader
	closed chan struct{}
	once   sync.Once
}

func newBufferConn(b []byte) *bufferConn {
	return &bufferConn{
		reader: bytes.NewReader(b),
		closed: make(chan struct{}),
	}
}

func (c *bufferConn) Read(p []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, io.ErrClosedPipe
	default:
		return c.reader.Read(p)
	}
}

func (c *bufferConn) Write(p []byte) (int, error) {
	select {
	case <-c.closed:
		return 0, io.ErrClosedPipe
	default:
		return len(p), nil
	}
}

func (c *bufferConn) Close() error {
	c.once.Do(func() {
		close(c.closed)
	})
	return nil
}

func (c *bufferConn) LocalAddr() net.Addr { return serverTestAddr("local") }

func (c *bufferConn) RemoteAddr() net.Addr { return serverTestAddr("remote") }

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

// 测试目的：已注册 MsgID 时，server 应把消息分发给对应 handler，并记录成功统计。
// 测试方法：直接调用 handleMsg，检查 handler 收到的 request 内容和 Success 统计。
func TestServerHandleMsgDispatchesAndRecordsSuccessStats(t *testing.T) {
	handler := newRecordingHandler(nil)
	srv := NewServer(
		newBlockingListener(),
		testAction{},
		WithHandlers(map[uint32]connection.Handler{7: handler}),
	)
	conn := connection.NewConnection(newBlockingConn(), "conn-data")

	srv.handleMsg(conn, &protocol.Message{
		MsgID: 7,
		Data:  []byte("payload"),
	})

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

	stat := srv.stats.GetStatistic(7)
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

// 测试目的：handler 返回错误时，server 应记录失败包数和失败字节数。
// 测试方法：让 recordingHandler 返回固定错误，然后检查 Failed 统计且 Success 为 0。
func TestServerHandleMsgRecordsFailedStats(t *testing.T) {
	wantErr := errors.New("handler failed")
	handler := newRecordingHandler(wantErr)
	srv := NewServer(
		newBlockingListener(),
		testAction{},
		WithHandlers(map[uint32]connection.Handler{8: handler}),
	)

	srv.handleMsg(
		connection.NewConnection(newBlockingConn(), nil),
		&protocol.Message{MsgID: 8, Data: []byte("bad")},
	)

	_ = receiveRequest(t, handler)
	stat := srv.stats.GetStatistic(8)
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
func TestServerHandleMsgUnknownIncrementsStats(t *testing.T) {
	srv := NewServer(newBlockingListener(), testAction{})

	srv.handleMsg(
		connection.NewConnection(newBlockingConn(), nil),
		&protocol.Message{MsgID: 404, Data: []byte("missing")},
	)

	if got := srv.stats.GetUnknownMsg(); got != 1 {
		t.Fatalf("UnknownMsg = %d, want 1", got)
	}
	stat := srv.stats.GetStatistic(404)
	if stat.SuccessPacket != 0 || stat.FailedPacket != 0 {
		t.Fatalf("unexpected stats for unknown message: %+v", stat)
	}
}

// 测试目的：WithHandlers 应复制传入的 map，避免 NewServer 后外部修改影响 server。
// 测试方法：创建 server 后修改原 map，再验证旧 handler 仍生效、新增 handler 不生效。
func TestServerWithHandlersClonesMap(t *testing.T) {
	first := newRecordingHandler(nil)
	second := newRecordingHandler(nil)
	handlers := map[uint32]connection.Handler{1: first}
	srv := NewServer(newBlockingListener(), testAction{}, WithHandlers(handlers))

	handlers[1] = second
	handlers[2] = second

	srv.handleMsg(
		connection.NewConnection(newBlockingConn(), nil),
		&protocol.Message{MsgID: 1, Data: []byte("one")},
	)
	_ = receiveRequest(t, first)
	if got := second.calls.Load(); got != 0 {
		t.Fatalf("mutated handler was called %d times, want 0", got)
	}

	srv.handleMsg(
		connection.NewConnection(newBlockingConn(), nil),
		&protocol.Message{MsgID: 2, Data: []byte("two")},
	)
	if got := srv.stats.GetUnknownMsg(); got != 1 {
		t.Fatalf("UnknownMsg = %d, want 1", got)
	}
	if got := second.calls.Load(); got != 0 {
		t.Fatalf("handler added after NewServer was called %d times, want 0", got)
	}
}

// 测试目的：重复调用 Start 不应启动第二组 accept goroutine。
// 测试方法：第一次 Start 指定 2 个 accept worker，第二次 Start 后确认 Accept 调用数仍为 2。
func TestServerStartIsIdempotent(t *testing.T) {
	listener := newBlockingListener()
	srv := NewServer(listener, testAction{})

	done := srv.Start(context.Background(), 2)
	if got := srv.Start(context.Background(), 2); got != done {
		t.Fatal("expected repeated Start to return the original done channel")
	}

	waitForAccepts(t, listener, 2)
	assertAcceptCountStays(t, listener, 2)

	waitForServerDone(t, srv.Stop())
}

// 测试目的：并发调用 Start 也只能完成一次初始化，并返回同一个 done channel。
// 测试方法：多个 goroutine 同时 Start，收集返回值并确认只有 1 个 accept worker 启动。
func TestServerConcurrentStartIsIdempotent(t *testing.T) {
	listener := newBlockingListener()
	srv := NewServer(listener, testAction{})

	const starters = 32
	type doneChannel <-chan struct{}
	doneCh := make(chan doneChannel, starters)
	var wg sync.WaitGroup
	wg.Add(starters)
	for i := 0; i < starters; i++ {
		go func() {
			defer wg.Done()
			doneCh <- srv.Start(context.Background(), 1)
		}()
	}
	wg.Wait()
	close(doneCh)

	var first <-chan struct{}
	for done := range doneCh {
		if done == nil {
			t.Fatal("Start returned nil done channel")
		}
		if first == nil {
			first = done
			continue
		}
		if done != first {
			t.Fatal("concurrent Start returned different done channels")
		}
	}

	waitForAccepts(t, listener, 1)
	assertAcceptCountStays(t, listener, 1)
	waitForServerDone(t, srv.Stop())
}

// 测试目的：acceptAmount 小于等于 0 时，Start 应按默认值 1 启动 accept worker。
// 测试方法：传入 0，确认只有一次 Accept 阻塞被启动。
func TestServerStartWithNonPositiveAcceptAmountUsesOneWorker(t *testing.T) {
	listener := newBlockingListener()
	srv := NewServer(listener, testAction{})

	_ = srv.Start(context.Background(), 0)

	waitForAccepts(t, listener, 1)
	assertAcceptCountStays(t, listener, 1)
	waitForServerDone(t, srv.Stop())
}

// 测试目的：Start 后调用 Stop 应返回同一个 done channel，并且 Stop 可重复调用。
// 测试方法：比较 Start/Stop/再次 Stop 返回的 channel，等待 server 正常退出。
func TestServerStopAfterStartReturnsDoneAndIsIdempotent(t *testing.T) {
	listener := newBlockingListener()
	srv := NewServer(listener, testAction{})
	done := srv.Start(context.Background(), 1)

	stopDone := srv.Stop()
	if stopDone != done {
		t.Fatal("Stop returned a different done channel")
	}
	waitForServerDone(t, stopDone)
	if got := srv.Stop(); got != done {
		t.Fatal("second Stop returned a different done channel")
	}
}

// 测试目的：父 context 取消时，server 应关闭 listener 并退出。
// 测试方法：Start 后取消父 context，等待 done 关闭并确认运行状态变为 false。
func TestServerStopsWhenParentContextCanceled(t *testing.T) {
	listener := newBlockingListener()
	srv := NewServer(listener, testAction{})
	ctx, cancel := context.WithCancel(context.Background())
	done := srv.Start(ctx, 1)

	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("server did not stop after parent context was canceled")
	}

	if srv.IsRunning() {
		t.Fatal("server is still marked running after parent context was canceled")
	}
}

// 测试目的：listener 被关闭导致 Accept 返回错误时，server 应停止运行。
// 测试方法：确认 Accept 已进入阻塞后关闭 listener，等待 done 并检查 IsRunning。
func TestServerStopsWhenListenerCloses(t *testing.T) {
	listener := newBlockingListener()
	srv := NewServer(listener, testAction{})
	done := srv.Start(context.Background(), 1)

	waitForAccepts(t, listener, 1)
	if err := listener.Close(); err != nil {
		t.Fatalf("listener close failed: %v", err)
	}

	waitForServerDone(t, done)
	if srv.IsRunning() {
		t.Fatal("server is still marked running after listener closed")
	}
}

// 测试目的：server 未 Start 前调用 Stop 不应创建 done channel。
// 测试方法：直接 Stop，期望返回 nil。
func TestServerStopBeforeStartReturnsNil(t *testing.T) {
	srv := NewServer(newBlockingListener(), testAction{})
	if done := srv.Stop(); done != nil {
		t.Fatal("expected nil done channel before server start")
	}
}

// 测试目的：超过 maxConnCount 的连接应被立即关闭，并且不影响已有连接。
// 测试方法：先投递一个阻塞连接占满额度，再投递第二个连接，验证第二个关闭、计数仍为 1。
func TestServerMaxConnCountClosesOverflowConnection(t *testing.T) {
	listener := newQueueListener(2)
	srv := NewServer(listener, testAction{}, WithMaxConnCount(1))
	first := newBlockingConn()
	second := newBlockingConn()

	_ = srv.Start(context.Background(), 1)
	listener.enqueue(first, nil)
	waitForReadStarted(t, first)

	listener.enqueue(second, nil)
	waitForConnClosed(t, second)
	assertConnOpen(t, first)

	if got := srv.connCount.Load(); got != 1 {
		t.Fatalf("connCount = %d, want 1", got)
	}

	waitForServerDone(t, srv.Stop())
	waitForConnClosed(t, first)
}

// 测试目的：超过 maxDataLen 的消息应被 decoder 拒绝，并通过 ConnErr 回调暴露错误。
// 测试方法：用较大上限编码 5 字节消息，再用 server 的 4 字节上限读取，检查 ErrDataLength。
func TestServerMaxDataLenRejectsOversizedMessage(t *testing.T) {
	var msg bytes.Buffer
	if err := protocol.NewDecoder(16).Marshal(&msg, 9, []byte("12345")); err != nil {
		t.Fatalf("marshal test message failed: %v", err)
	}

	listener := newQueueListener(1)
	action := newRecordingAction()
	srv := NewServer(listener, action, WithMaxDataLen(4))

	_ = srv.Start(context.Background(), 1)
	listener.enqueue(newBufferConn(msg.Bytes()), nil)

	err := receiveConnErr(t, action)
	if !errors.Is(err, protocol.ErrDataLength) {
		t.Fatalf("ConnErr = %v, want %v", err, protocol.ErrDataLength)
	}

	waitForServerDone(t, srv.Stop())
}

// 测试目的：测试用的 blockingListener Close 必须幂等，避免多个停止路径重复关闭 channel panic。
// 测试方法：连续 Close 两次后调用 Accept，确认返回 net.ErrClosed。
func TestBlockingListenerCloseIsIdempotent(t *testing.T) {
	listener := newBlockingListener()
	if err := listener.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("second close failed: %v", err)
	}
	_, _, err := listener.Accept()
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("expected net.ErrClosed, got %v", err)
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

func waitForAccepts(t *testing.T, listener *blockingListener, want int) {
	t.Helper()

	for i := 0; i < want; i++ {
		select {
		case <-listener.acceptStarted:
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("listener Accept called %d times, want %d", i, want)
		}
	}
}

func assertAcceptCountStays(t *testing.T, listener *blockingListener, want int32) {
	t.Helper()

	time.Sleep(50 * time.Millisecond)
	if got := listener.acceptCount.Load(); got != want {
		t.Fatalf("accept count = %d, want %d", got, want)
	}
}

func waitForServerDone(t *testing.T, done <-chan struct{}) {
	t.Helper()

	if done == nil {
		t.Fatal("server returned nil done channel")
	}
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("server did not stop")
	}
}

func waitForReadStarted(t *testing.T, conn *blockingConn) {
	t.Helper()

	select {
	case <-conn.readStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("connection read did not start")
	}
}

func waitForConnClosed(t *testing.T, conn *blockingConn) {
	t.Helper()

	select {
	case <-conn.closed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("connection was not closed")
	}
}

func assertConnOpen(t *testing.T, conn *blockingConn) {
	t.Helper()

	select {
	case <-conn.closed:
		t.Fatal("connection was closed unexpectedly")
	default:
	}
}

func receiveConnErr(t *testing.T, action *recordingAction) error {
	t.Helper()

	select {
	case err := <-action.connErrs:
		return err
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ConnErr callback was not called")
		return nil
	}
}
