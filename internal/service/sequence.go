package service

import (
	"sync"
	"sync/atomic"
)

type memorySequencer struct {
	nextMessageID atomic.Int64
	mu            sync.Mutex
	userSeq       map[string]uint32
}

func (s *memorySequencer) NextMessageID() int64 {
	if s == nil {
		return 0
	}
	return s.nextMessageID.Add(1)
}

func (s *memorySequencer) NextChannelSequence(channelKey string) uint32 {
	if s == nil {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.userSeq == nil {
		s.userSeq = make(map[string]uint32)
	}

	next := s.userSeq[channelKey] + 1
	s.userSeq[channelKey] = next
	return next
}
