package runtime

import (
	"sync"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
)

type peerSessionCache struct {
	mu       sync.Mutex
	sessions map[core.NodeID]PeerSession
}

func newPeerSessionCache() peerSessionCache {
	return peerSessionCache{
		sessions: make(map[core.NodeID]PeerSession),
	}
}

func (r *runtime) peerSession(peer core.NodeID) PeerSession {
	if r.isClosed() {
		return nopPeerSession{}
	}
	r.sessions.mu.Lock()
	defer r.sessions.mu.Unlock()

	if session, ok := r.sessions.sessions[peer]; ok {
		return session
	}
	session := r.cfg.PeerSessions.Session(peer)
	r.sessions.sessions[peer] = session
	return session
}
