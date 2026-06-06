package server

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jursonmo/simple-message/connection"
)

type testAction struct{}

func (testAction) ConnErr(ctx context.Context, conn *connection.Connection, err error) {}

func (testAction) ConnectedBegin(ctx context.Context, conn *connection.Connection) {}

type blockingListener struct {
	closed chan struct{}
	once   sync.Once
}

func newBlockingListener() *blockingListener {
	return &blockingListener{
		closed: make(chan struct{}),
	}
}

func (l *blockingListener) Accept() (connection.Conn, any, error) {
	<-l.closed
	return nil, nil, net.ErrClosed
}

func (l *blockingListener) Close() error {
	l.once.Do(func() {
		close(l.closed)
	})
	return nil
}

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

func TestServerStopBeforeStartReturnsNil(t *testing.T) {
	srv := NewServer(newBlockingListener(), testAction{})
	if done := srv.Stop(); done != nil {
		t.Fatal("expected nil done channel before server start")
	}
}

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
