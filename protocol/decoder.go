package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var (
	ErrInvalidWrite = errors.New("invalid write result")
	ErrDataLength   = errors.New("数据长度过大")
)

// 定义协议头各部分的长度
const (
	HeaderDataLen = uint32(4) ///头部长度
	DataSizeLen   = uint32(4) // 数据大小字段的长度，单位为字节
	ReadLen       = HeaderDataLen + DataSizeLen
	MaxDataLen    = uint32(1024 * 1024 * 2) // 默认最大数据长度 (2MB)
)

type Decoder struct {
	maxDataLen uint32
}

func NewDecoder(maxDataLen uint32) *Decoder {
	d := &Decoder{}
	if maxDataLen == 0 {
		maxDataLen = MaxDataLen
	}
	d.maxDataLen = maxDataLen
	return d
}

func (d *Decoder) Unmarshal(conn io.Reader) (*Message, error) {
	buf := make([]byte, ReadLen)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, err
	}
	// 从缓冲区的第 2 到 3 字节获取数据大小
	MsgID := binary.BigEndian.Uint32(buf[0:HeaderDataLen])
	dataSize := binary.BigEndian.Uint32(buf[HeaderDataLen:ReadLen])
	if dataSize > d.maxDataLen {
		return nil, fmt.Errorf("%w 不得大于%d", ErrDataLength, d.maxDataLen)
	}
	buf = make([]byte, dataSize)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, err
	}
	r := &Message{
		MsgID: MsgID,
		Data:  buf,
	}
	return r, nil
}

func (d *Decoder) Marshal(conn io.Writer, MsgID uint32, data []byte) error {
	n := uint32(len(data))
	if n > d.maxDataLen {
		return fmt.Errorf("%w 不得大于%d", ErrDataLength, d.maxDataLen)
	}
	b := make([]byte, ReadLen+n)
	binary.BigEndian.PutUint32(b[0:HeaderDataLen], uint32(MsgID))
	binary.BigEndian.PutUint32(b[HeaderDataLen:ReadLen], uint32(n))
	copy(b[ReadLen:], data)
	_, err := conn.Write(b)
	return err
}
