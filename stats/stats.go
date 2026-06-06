package stats

import (
	"sync"
	"sync/atomic"
)

type HandlerStats struct {
	sync.RWMutex
	UnknownMsg int64
	Statistic  map[uint32]*HandlerStatistic
}

type HandlerStatistic struct {
	sync.Mutex
	MsgID         uint32
	SuccessPacket int64
	SuccessBytes  uint64

	FailedPacket int64
	FailedBytes  uint64
}

func NewHandlerStats() *HandlerStats {
	return &HandlerStats{
		Statistic: make(map[uint32]*HandlerStatistic),
	}
}

func (s *HandlerStats) GetStatistic(MsgID uint32) HandlerStatistic {
	s.RLock()
	stat, ok := s.Statistic[MsgID]
	s.RUnlock()

	if !ok {
		return HandlerStatistic{
			MsgID: MsgID,
		}
	}

	stat.Lock()
	defer stat.Unlock()
	return HandlerStatistic{
		MsgID:         stat.MsgID,
		SuccessPacket: stat.SuccessPacket,
		SuccessBytes:  stat.SuccessBytes,
		FailedPacket:  stat.FailedPacket,
		FailedBytes:   stat.FailedBytes,
	}
}

func (s *HandlerStats) getOrCreateStatistic(MsgID uint32) *HandlerStatistic {
	s.Lock()
	defer s.Unlock()

	if stat, ok := s.Statistic[MsgID]; ok {
		return stat
	}

	stat := &HandlerStatistic{
		MsgID: MsgID,
	}
	s.Statistic[MsgID] = stat
	return stat
}

func (s *HandlerStats) GetUnknownMsg() int64 {
	return atomic.LoadInt64(&s.UnknownMsg)
}

func (s *HandlerStats) AddSuccessBytes(MsgID uint32, bytes uint64) {
	s.AddBytes(MsgID, bytes, true)
}
func (s *HandlerStats) AddFailedBytes(MsgID uint32, bytes uint64) {
	s.AddBytes(MsgID, bytes, false)
}

func (s *HandlerStats) AddBytes(MsgID uint32, bytes uint64, success bool) {
	stat := s.getOrCreateStatistic(MsgID)

	stat.Lock()
	defer stat.Unlock()
	if success {
		stat.SuccessPacket++
		stat.SuccessBytes += bytes
		return
	}

	stat.FailedPacket++
	stat.FailedBytes += bytes
}

func (s *HandlerStats) IncUnknownMsg() {
	atomic.AddInt64(&s.UnknownMsg, 1)
}
