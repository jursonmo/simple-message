package server

import (
	"github.com/jursonmo/simple-message/connection"
)

type Listener interface {
	Accept() (connection.Conn, any, error)
	Close() error
}
