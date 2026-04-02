package multiisr

import (
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/isr"
)

type peerSessionCache struct {
	mu       sync.Mutex
	sessions map[isr.NodeID]PeerSession
}

func newPeerSessionCache() peerSessionCache {
	return peerSessionCache{
		sessions: make(map[isr.NodeID]PeerSession),
	}
}

func (r *runtime) peerSession(peer isr.NodeID) PeerSession {
	r.sessions.mu.Lock()
	defer r.sessions.mu.Unlock()

	if session, ok := r.sessions.sessions[peer]; ok {
		return session
	}
	session := r.cfg.PeerSessions.Session(peer)
	r.sessions.sessions[peer] = session
	return session
}
