package stats

import (
	"sync"
	"testing"
)

func TestAddBytesConcurrentSameMsgIDDoesNotLoseCounts(t *testing.T) {
	s := NewHandlerStats()
	const (
		msgID      = uint32(7)
		goroutines = 128
		iterations = 1000
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				s.AddSuccessBytes(msgID, 2)
				s.AddFailedBytes(msgID, 3)
			}
		}()
	}

	close(start)
	wg.Wait()

	got := s.GetStatistic(msgID)
	wantPacket := int64(goroutines * iterations)
	if got.SuccessPacket != wantPacket {
		t.Fatalf("SuccessPacket = %d, want %d", got.SuccessPacket, wantPacket)
	}
	if got.SuccessBytes != uint64(wantPacket*2) {
		t.Fatalf("SuccessBytes = %d, want %d", got.SuccessBytes, wantPacket*2)
	}
	if got.FailedPacket != wantPacket {
		t.Fatalf("FailedPacket = %d, want %d", got.FailedPacket, wantPacket)
	}
	if got.FailedBytes != uint64(wantPacket*3) {
		t.Fatalf("FailedBytes = %d, want %d", got.FailedBytes, wantPacket*3)
	}
}

func TestGetStatisticConcurrentWithAddBytes(t *testing.T) {
	s := NewHandlerStats()
	const (
		msgID      = uint32(9)
		writers    = 16
		readers    = 16
		iterations = 1000
	)

	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(writers + readers)

	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				s.AddSuccessBytes(msgID, 1)
			}
		}()
	}

	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				_ = s.GetStatistic(msgID)
			}
		}()
	}

	close(start)
	wg.Wait()

	got := s.GetStatistic(msgID)
	wantPacket := int64(writers * iterations)
	if got.SuccessPacket != wantPacket {
		t.Fatalf("SuccessPacket = %d, want %d", got.SuccessPacket, wantPacket)
	}
}

func TestIncUnknownMsgConcurrent(t *testing.T) {
	s := NewHandlerStats()
	const (
		goroutines = 64
		iterations = 1000
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				s.IncUnknownMsg()
			}
		}()
	}
	wg.Wait()

	want := int64(goroutines * iterations)
	if got := s.GetUnknownMsg(); got != want {
		t.Fatalf("UnknownMsg = %d, want %d", got, want)
	}
}
