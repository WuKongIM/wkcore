package multiisr

import (
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/isr"
)

type peerRequestState struct {
	mu       sync.Mutex
	inflight map[isr.NodeID]int
	queued   map[isr.NodeID][]Envelope
}

func newPeerRequestState() peerRequestState {
	return peerRequestState{
		inflight: make(map[isr.NodeID]int),
		queued:   make(map[isr.NodeID][]Envelope),
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
	s.queued[env.Peer] = append(s.queued[env.Peer], env)
}

func (s *peerRequestState) queuedCount(peer isr.NodeID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queued[peer])
}

func (s *peerRequestState) release(peer isr.NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight[peer] > 0 {
		s.inflight[peer]--
	}
}

func (s *peerRequestState) popQueued(peer isr.NodeID) (Envelope, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	queue := s.queued[peer]
	if len(queue) == 0 {
		return Envelope{}, false
	}
	env := queue[0]
	if len(queue) == 1 {
		delete(s.queued, peer)
	} else {
		s.queued[peer] = queue[1:]
	}
	return env, true
}
