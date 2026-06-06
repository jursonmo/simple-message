package connection

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

type testAddr string

func (a testAddr) Network() string { return "test" }

func (a testAddr) String() string { return string(a) }

type testConn struct{}

func (testConn) Read(p []byte) (int, error) { return 0, io.EOF }

func (testConn) Write(p []byte) (int, error) { return len(p), nil }

func (testConn) Close() error { return nil }

func (testConn) LocalAddr() net.Addr { return testAddr("local") }

func (testConn) RemoteAddr() net.Addr { return testAddr("remote") }

func TestSendMsgContextReturnsWhenConnectionClosesAfterEnqueue(t *testing.T) {
	conn := NewConnection(testConn{}, nil)
	errCh := make(chan error, 1)

	go func() {
		errCh <- conn.SendMsgContext(context.Background(), 1, []byte("queued"))
	}()

	receiveQueuedMessage(t, conn)
	conn.Close()

	if err := receiveSendErr(t, errCh); !errors.Is(err, ErrIsClose) {
		t.Fatalf("expected ErrIsClose, got %v", err)
	}
}

func TestSendMsgContextReturnsWhenContextCancelsAfterEnqueue(t *testing.T) {
	conn := NewConnection(testConn{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- conn.SendMsgContext(ctx, 1, []byte("queued"))
	}()

	receiveQueuedMessage(t, conn)
	cancel()

	if err := receiveSendErr(t, errCh); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestSendMsgContextReturnsAckError(t *testing.T) {
	conn := NewConnection(testConn{}, nil)
	errCh := make(chan error, 1)
	wantErr := errors.New("write failed")

	go func() {
		errCh <- conn.SendMsgContext(context.Background(), 1, []byte("queued"))
	}()

	msg := receiveQueuedMessage(t, conn)
	msg.AckMessage(func() error {
		return wantErr
	})

	if err := receiveSendErr(t, errCh); !errors.Is(err, wantErr) {
		t.Fatalf("expected ack error %v, got %v", wantErr, err)
	}
}

func receiveQueuedMessage(t *testing.T, conn *Connection) *MessageBody {
	t.Helper()

	select {
	case msg := <-conn.msgChan:
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
