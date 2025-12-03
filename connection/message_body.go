package connection

import (
	"sync/atomic"

	"github.com/jursonmo/simple-message/protocol"
)

type T func() error

type MessageBody struct {
	message *protocol.Message
	err     error
	ackChan chan struct{}
	status  atomic.Bool
}

func NewMessageBody(message *protocol.Message) *MessageBody {
	return &MessageBody{
		message: message,
		ackChan: make(chan struct{}),
	}
}

func (m *MessageBody) GetMessage() *protocol.Message {
	return m.message
}

func (m *MessageBody) Done() <-chan struct{} {
	return m.ackChan
}

func (m *MessageBody) AckMessage(t T) {
	if m.status.CompareAndSwap(false, true) {
		m.err = t()
		close(m.ackChan)
	}
}
