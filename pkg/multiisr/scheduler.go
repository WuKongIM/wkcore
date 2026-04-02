package multiisr

import "sync"

type taskMask uint8

const (
	taskControl taskMask = 1 << iota
	taskReplication
	taskCommit
	taskLease
	taskSnapshot
)

type scheduler struct {
	ch chan uint64

	mu         sync.Mutex
	pending    []uint64
	queued     map[uint64]struct{}
	processing map[uint64]struct{}
	dirty      map[uint64]struct{}
}

func newScheduler() *scheduler {
	return &scheduler{
		ch:         make(chan uint64, 1024),
		queued:     make(map[uint64]struct{}),
		processing: make(map[uint64]struct{}),
		dirty:      make(map[uint64]struct{}),
	}
}

func (s *scheduler) enqueue(groupID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.queued[groupID]; ok {
		return
	}
	if _, ok := s.processing[groupID]; ok {
		s.dirty[groupID] = struct{}{}
		return
	}
	s.queued[groupID] = struct{}{}
	s.pending = append(s.pending, groupID)
	s.dispatchLocked()
}

func (s *scheduler) begin(groupID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.queued, groupID)
	s.processing[groupID] = struct{}{}
	s.dispatchLocked()
}

func (s *scheduler) done(groupID uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.processing, groupID)
	if _, ok := s.dirty[groupID]; !ok {
		return false
	}
	delete(s.dirty, groupID)
	return true
}

func (s *scheduler) requeue(groupID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.queued[groupID]; ok {
		return
	}
	s.queued[groupID] = struct{}{}
	s.pending = append(s.pending, groupID)
	s.dispatchLocked()
}

func (s *scheduler) dispatchLocked() {
	for len(s.pending) > 0 {
		select {
		case s.ch <- s.pending[0]:
			s.pending = s.pending[1:]
		default:
			return
		}
	}
}
