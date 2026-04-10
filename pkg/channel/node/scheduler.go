package node

import (
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
)

type taskMask uint8

const (
	taskControl taskMask = 1 << iota
	taskReplication
	taskCommit
	taskLease
	taskSnapshot
)

type scheduler struct {
	ch chan isr.GroupKey

	mu         sync.Mutex
	pending    schedulerQueue
	queued     map[isr.GroupKey]struct{}
	processing map[isr.GroupKey]struct{}
	dirty      map[isr.GroupKey]struct{}
}

type schedulerQueue struct {
	items []isr.GroupKey
	head  int
}

func newScheduler() *scheduler {
	return &scheduler{
		ch:         make(chan isr.GroupKey, 1024),
		queued:     make(map[isr.GroupKey]struct{}),
		processing: make(map[isr.GroupKey]struct{}),
		dirty:      make(map[isr.GroupKey]struct{}),
	}
}

func (s *scheduler) enqueue(groupKey isr.GroupKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.queued[groupKey]; ok {
		return
	}
	if _, ok := s.processing[groupKey]; ok {
		s.dirty[groupKey] = struct{}{}
		return
	}
	s.queued[groupKey] = struct{}{}
	s.pending.enqueue(groupKey)
	s.dispatchLocked()
}

func (s *scheduler) begin(groupKey isr.GroupKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.queued, groupKey)
	s.processing[groupKey] = struct{}{}
	s.dispatchLocked()
}

func (s *scheduler) done(groupKey isr.GroupKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.processing, groupKey)
	if _, ok := s.dirty[groupKey]; !ok {
		return false
	}
	delete(s.dirty, groupKey)
	return true
}

func (s *scheduler) requeue(groupKey isr.GroupKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.queued[groupKey]; ok {
		return
	}
	s.queued[groupKey] = struct{}{}
	s.pending.enqueue(groupKey)
	s.dispatchLocked()
}

func (s *scheduler) dispatchLocked() {
	for {
		groupKey, ok := s.pending.pop()
		if !ok {
			return
		}
		select {
		case s.ch <- groupKey:
		default:
			s.pending.unshift(groupKey)
			return
		}
	}
}

func (q *schedulerQueue) enqueue(groupKey isr.GroupKey) {
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	} else if q.head > 0 && len(q.items) == cap(q.items) {
		q.compact()
	}
	q.items = append(q.items, groupKey)
}

func (q *schedulerQueue) pop() (isr.GroupKey, bool) {
	if q.head >= len(q.items) {
		return "", false
	}

	groupKey := q.items[q.head]
	q.items[q.head] = ""
	q.head++
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	}
	return groupKey, true
}

func (q *schedulerQueue) unshift(groupKey isr.GroupKey) {
	if q.head > 0 {
		q.head--
		q.items[q.head] = groupKey
		return
	}

	q.items = append(q.items, "")
	copy(q.items[1:], q.items[:len(q.items)-1])
	q.items[0] = groupKey
}

func (q *schedulerQueue) compact() {
	n := copy(q.items, q.items[q.head:])
	for i := n; i < len(q.items); i++ {
		q.items[i] = ""
	}
	q.items = q.items[:n]
	q.head = 0
}
