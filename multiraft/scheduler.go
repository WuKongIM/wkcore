package multiraft

import "sync"

type scheduler struct {
	ch chan GroupID

	mu       sync.Mutex
	enqueued map[GroupID]struct{}
}

func newScheduler() *scheduler {
	return &scheduler{
		ch:       make(chan GroupID, 1024),
		enqueued: make(map[GroupID]struct{}),
	}
}

func (s *scheduler) enqueue(groupID GroupID) {
	s.mu.Lock()
	if _, ok := s.enqueued[groupID]; ok {
		s.mu.Unlock()
		return
	}
	s.enqueued[groupID] = struct{}{}
	s.mu.Unlock()

	s.ch <- groupID
}

func (s *scheduler) done(groupID GroupID) {
	s.mu.Lock()
	delete(s.enqueued, groupID)
	s.mu.Unlock()
}
