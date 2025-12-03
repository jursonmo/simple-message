package server

import (
	"context"

	"github.com/jursonmo/simple-message/connection"
)

type Action interface {
	ConnErr(ctx context.Context, conn *connection.Connection, err error) ///连接错误回调
	ConnectedBegin(ctx context.Context, conn *connection.Connection)     // 连接建立回调
}
