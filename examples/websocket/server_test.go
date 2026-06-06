package websocket

import (
	"errors"
	"net"
	"testing"
)

func TestAcceptReturnsErrClosedWhenConnChanClosed(t *testing.T) {
	listener := &WebSocketListener{
		connChan:  make(chan *Conn),
		closeChan: make(chan struct{}),
	}
	close(listener.connChan)

	conn, data, err := listener.Accept()
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("expected net.ErrClosed, got %v", err)
	}
	if conn != nil {
		t.Fatalf("expected nil conn, got %v", conn)
	}
	if data != nil {
		t.Fatalf("expected nil data, got %v", data)
	}
}
