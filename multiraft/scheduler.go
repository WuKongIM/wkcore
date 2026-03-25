package multiraft

import "sync"

type scheduler struct {
	ch chan GroupID

	mu         sync.Mutex
	queued     map[GroupID]struct{}
	processing map[GroupID]struct{}
	dirty      map[GroupID]struct{}
}

func newScheduler() *scheduler {
	return &scheduler{
		ch:         make(chan GroupID, 1024),
		queued:     make(map[GroupID]struct{}),
		processing: make(map[GroupID]struct{}),
		dirty:      make(map[GroupID]struct{}),
	}
}

func (s *scheduler) enqueue(groupID GroupID) {
	s.mu.Lock()
	if _, ok := s.queued[groupID]; ok {
		s.mu.Unlock()
		return
	}
	if _, ok := s.processing[groupID]; ok {
		s.dirty[groupID] = struct{}{}
		s.mu.Unlock()
		return
	}
	s.queued[groupID] = struct{}{}
	s.mu.Unlock()

	s.ch <- groupID
}

func (s *scheduler) begin(groupID GroupID) {
	s.mu.Lock()
	delete(s.queued, groupID)
	s.processing[groupID] = struct{}{}
	s.mu.Unlock()
}

func (s *scheduler) done(groupID GroupID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.processing, groupID)
	if _, ok := s.dirty[groupID]; !ok {
		return false
	}
	delete(s.dirty, groupID)
	s.queued[groupID] = struct{}{}
	return true
}

func (s *scheduler) requeue(groupID GroupID) {
	s.mu.Lock()
	if _, ok := s.queued[groupID]; ok {
		s.mu.Unlock()
		return
	}
	s.queued[groupID] = struct{}{}
	s.mu.Unlock()

	s.ch <- groupID
}
