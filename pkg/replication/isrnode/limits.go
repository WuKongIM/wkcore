package isrnode

import (
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

type peerRequestState struct {
	mu       sync.Mutex
	inflight map[isr.NodeID]int
	groups   map[groupPeerKey]struct{}
	queued   map[isr.NodeID]*peerEnvelopeQueue
}

type peerEnvelopeQueue struct {
	items []Envelope
	head  int
}

type groupPeerKey struct {
	groupKey isr.GroupKey
	peer     isr.NodeID
}

func newPeerRequestState() peerRequestState {
	return peerRequestState{
		inflight: make(map[isr.NodeID]int),
		groups:   make(map[groupPeerKey]struct{}),
		queued:   make(map[isr.NodeID]*peerEnvelopeQueue),
	}
}

func (s *peerRequestState) tryAcquire(peer isr.NodeID, limit int) bool {
	if limit <= 0 {
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight[peer] >= limit {
		return false
	}
	s.inflight[peer]++
	return true
}

func (s *peerRequestState) enqueue(env Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue := s.queueLocked(env.Peer)
	queue.enqueue(env)
}

func (s *peerRequestState) tryAcquireGroup(groupKey isr.GroupKey, peer isr.NodeID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := groupPeerKey{groupKey: groupKey, peer: peer}
	if _, ok := s.groups[key]; ok {
		return false
	}
	s.groups[key] = struct{}{}
	return true
}

func (s *peerRequestState) queuedCount(peer isr.NodeID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue := s.queued[peer]
	if queue == nil {
		return 0
	}
	return queue.len()
}

func (s *peerRequestState) release(peer isr.NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight[peer] > 0 {
		s.inflight[peer]--
	}
}

func (s *peerRequestState) releaseGroup(groupKey isr.GroupKey, peer isr.NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.groups, groupPeerKey{groupKey: groupKey, peer: peer})
}

func (s *peerRequestState) popQueued(peer isr.NodeID) (Envelope, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue := s.queued[peer]
	if queue == nil {
		return Envelope{}, false
	}
	return queue.pop()
}

func (s *peerRequestState) queueLocked(peer isr.NodeID) *peerEnvelopeQueue {
	if queue, ok := s.queued[peer]; ok {
		return queue
	}
	queue := &peerEnvelopeQueue{}
	s.queued[peer] = queue
	return queue
}

func (q *peerEnvelopeQueue) enqueue(env Envelope) {
	if env.Kind == MessageKindFetchRequest {
		for i := q.head; i < len(q.items); i++ {
			if q.items[i].Kind == MessageKindFetchRequest && q.items[i].GroupKey == env.GroupKey {
				q.items[i] = env
				return
			}
		}
	}
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	} else if q.head > 0 && len(q.items) == cap(q.items) {
		q.compact()
	}
	q.items = append(q.items, env)
}

func (q *peerEnvelopeQueue) len() int {
	return len(q.items) - q.head
}

func (q *peerEnvelopeQueue) pop() (Envelope, bool) {
	if q.head >= len(q.items) {
		return Envelope{}, false
	}

	env := q.items[q.head]
	q.items[q.head] = Envelope{}
	q.head++
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	}
	return env, true
}

func (q *peerEnvelopeQueue) compact() {
	n := copy(q.items, q.items[q.head:])
	for i := n; i < len(q.items); i++ {
		q.items[i] = Envelope{}
	}
	q.items = q.items[:n]
	q.head = 0
}
