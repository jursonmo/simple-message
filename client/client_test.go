package client

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jursonmo/simple-message/connection"
)

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

func TestClientStartIsIdempotent(t *testing.T) {
	action := newBlockingAction()
	c := NewClient(action)

	c.Start(context.Background())
	c.Start(context.Background())

	waitForDial(t, action)
	assertDialCountStays(t, action, 1)

	done := c.Stop()
	if done == nil {
		t.Fatal("expected non-nil done channel after Start")
	}
	waitForClientDone(t, done)
}

func TestClientConcurrentStartIsIdempotent(t *testing.T) {
	action := newBlockingAction()
	c := NewClient(action)

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

	waitForDial(t, action)
	assertDialCountStays(t, action, 1)

	waitForClientDone(t, c.Stop())
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

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("client did not stop")
	}
}
