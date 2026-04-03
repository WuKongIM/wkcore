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
	pending    schedulerQueue
	queued     map[uint64]struct{}
	processing map[uint64]struct{}
	dirty      map[uint64]struct{}
}

type schedulerQueue struct {
	items []uint64
	head  int
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
	s.pending.enqueue(groupID)
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
	s.pending.enqueue(groupID)
	s.dispatchLocked()
}

func (s *scheduler) dispatchLocked() {
	for {
		groupID, ok := s.pending.pop()
		if !ok {
			return
		}
		select {
		case s.ch <- groupID:
		default:
			s.pending.unshift(groupID)
			return
		}
	}
}

func (q *schedulerQueue) enqueue(groupID uint64) {
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	} else if q.head > 0 && len(q.items) == cap(q.items) {
		q.compact()
	}
	q.items = append(q.items, groupID)
}

func (q *schedulerQueue) pop() (uint64, bool) {
	if q.head >= len(q.items) {
		return 0, false
	}

	groupID := q.items[q.head]
	q.items[q.head] = 0
	q.head++
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	}
	return groupID, true
}

func (q *schedulerQueue) unshift(groupID uint64) {
	if q.head > 0 {
		q.head--
		q.items[q.head] = groupID
		return
	}

	q.items = append(q.items, 0)
	copy(q.items[1:], q.items[:len(q.items)-1])
	q.items[0] = groupID
}

func (q *schedulerQueue) compact() {
	n := copy(q.items, q.items[q.head:])
	for i := n; i < len(q.items); i++ {
		q.items[i] = 0
	}
	q.items = q.items[:n]
	q.head = 0
}
