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
