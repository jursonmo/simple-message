package client

import (
	"context"

	"github.com/jursonmo/simple-message/connection"
)

type Action interface {
	DialContext(context.Context) (connection.Conn, any, error)           // 拨号函数
	ConnErr(ctx context.Context, conn *connection.Connection, err error) // 连接断连回调函数
	ConnectedBegin(ctx context.Context, conn *connection.Connection)     // 拨号成功后开始处理回调函数
}
