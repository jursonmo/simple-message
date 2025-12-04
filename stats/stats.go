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
	defer s.RUnlock()
	if stat, ok := s.Statistic[MsgID]; ok {
		return HandlerStatistic{
			MsgID:         stat.MsgID,
			SuccessPacket: stat.SuccessPacket,
			SuccessBytes:  stat.SuccessBytes,
			FailedPacket:  stat.FailedPacket,
			FailedBytes:   stat.FailedBytes,
		}
	}
	return HandlerStatistic{
		MsgID: MsgID,
	}
}

func (s *HandlerStats) AddSuccessBytes(MsgID uint32, bytes uint64) {
	s.AddBytes(MsgID, bytes, true)
}
func (s *HandlerStats) AddFailedBytes(MsgID uint32, bytes uint64) {
	s.AddBytes(MsgID, bytes, false)
}

func (s *HandlerStats) AddBytes(MsgID uint32, bytes uint64, success bool) {
	s.RLock()
	stat, ok := s.Statistic[MsgID]
	s.RUnlock()

	if ok {
		stat.Lock()
		defer stat.Unlock()
		if success {
			stat.SuccessPacket++
			stat.SuccessBytes += bytes
			return
		}

		stat.FailedPacket++
		stat.FailedBytes += bytes
		return
	}

	hs := &HandlerStatistic{
		MsgID: MsgID,
	}
	if !success {
		hs.FailedPacket = 1
		hs.FailedBytes = bytes
	} else {
		hs.SuccessPacket = 1
		hs.SuccessBytes = bytes
	}

	s.Lock()
	s.Statistic[MsgID] = hs
	s.Unlock()
}
func (s *HandlerStats) IncUnknownMsg() {
	atomic.AddInt64(&s.UnknownMsg, 1)
}
