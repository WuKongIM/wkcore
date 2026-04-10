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
	ch chan isr.ChannelKey

	mu         sync.Mutex
	pending    schedulerQueue
	queued     map[isr.ChannelKey]struct{}
	processing map[isr.ChannelKey]struct{}
	dirty      map[isr.ChannelKey]struct{}
}

type schedulerQueue struct {
	items []isr.ChannelKey
	head  int
}

func newScheduler() *scheduler {
	return &scheduler{
		ch:         make(chan isr.ChannelKey, 1024),
		queued:     make(map[isr.ChannelKey]struct{}),
		processing: make(map[isr.ChannelKey]struct{}),
		dirty:      make(map[isr.ChannelKey]struct{}),
	}
}

func (s *scheduler) enqueue(channelKey isr.ChannelKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.queued[channelKey]; ok {
		return
	}
	if _, ok := s.processing[channelKey]; ok {
		s.dirty[channelKey] = struct{}{}
		return
	}
	s.queued[channelKey] = struct{}{}
	s.pending.enqueue(channelKey)
	s.dispatchLocked()
}

func (s *scheduler) begin(channelKey isr.ChannelKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.queued, channelKey)
	s.processing[channelKey] = struct{}{}
	s.dispatchLocked()
}

func (s *scheduler) done(channelKey isr.ChannelKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.processing, channelKey)
	if _, ok := s.dirty[channelKey]; !ok {
		return false
	}
	delete(s.dirty, channelKey)
	return true
}

func (s *scheduler) requeue(channelKey isr.ChannelKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.queued[channelKey]; ok {
		return
	}
	s.queued[channelKey] = struct{}{}
	s.pending.enqueue(channelKey)
	s.dispatchLocked()
}

func (s *scheduler) dispatchLocked() {
	for {
		channelKey, ok := s.pending.pop()
		if !ok {
			return
		}
		select {
		case s.ch <- channelKey:
		default:
			s.pending.unshift(channelKey)
			return
		}
	}
}

func (q *schedulerQueue) enqueue(channelKey isr.ChannelKey) {
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	} else if q.head > 0 && len(q.items) == cap(q.items) {
		q.compact()
	}
	q.items = append(q.items, channelKey)
}

func (q *schedulerQueue) pop() (isr.ChannelKey, bool) {
	if q.head >= len(q.items) {
		return "", false
	}

	channelKey := q.items[q.head]
	q.items[q.head] = ""
	q.head++
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	}
	return channelKey, true
}

func (q *schedulerQueue) unshift(channelKey isr.ChannelKey) {
	if q.head > 0 {
		q.head--
		q.items[q.head] = channelKey
		return
	}

	q.items = append(q.items, "")
	copy(q.items[1:], q.items[:len(q.items)-1])
	q.items[0] = channelKey
}

func (q *schedulerQueue) compact() {
	n := copy(q.items, q.items[q.head:])
	for i := n; i < len(q.items); i++ {
		q.items[i] = ""
	}
	q.items = q.items[:n]
	q.head = 0
}
