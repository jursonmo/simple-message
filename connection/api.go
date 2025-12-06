package connection

import (
	"context"
	"errors"
	"io"
	"net"
)

type Conn interface {
	io.ReadWriteCloser
	LocalAddr() net.Addr //增加这两个方法，用于获取本地和远程地址，方便打印。
	RemoteAddr() net.Addr
}

var (
	ErrIsClose     = errors.New("已关闭")
	ErrKeyNotFound = errors.New("属性不存在")
)

type ConnectedBegin func(ctx context.Context, conn *Connection)
