package isrnodetransport

import (
	"context"
	"fmt"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isrnode"
)

const fetchRPCShardKey uint64 = 0

type sessionManager struct {
	adapter *Adapter

	mu       sync.Mutex
	sessions map[isr.NodeID]*peerSession
}

type peerSession struct {
	adapter *Adapter
	peer    isr.NodeID

	mu              sync.Mutex
	pendingRequests int
	pendingBytes    int64
}

var _ isrnode.PeerSessionManager = (*sessionManager)(nil)
var _ isrnode.PeerSession = (*peerSession)(nil)

func newSessionManager(adapter *Adapter) *sessionManager {
	return &sessionManager{
		adapter:  adapter,
		sessions: make(map[isr.NodeID]*peerSession),
	}
}

func (m *sessionManager) Session(peer isr.NodeID) isrnode.PeerSession {
	m.mu.Lock()
	defer m.mu.Unlock()

	if session, ok := m.sessions[peer]; ok {
		return session
	}
	session := &peerSession{adapter: m.adapter, peer: peer}
	m.sessions[peer] = session
	return session
}

func (s *peerSession) Send(env isrnode.Envelope) error {
	if env.Kind != isrnode.MessageKindFetchRequest {
		return fmt.Errorf("isrnodetransport: unsupported envelope kind %d", env.Kind)
	}
	if env.FetchRequest == nil {
		return fmt.Errorf("isrnodetransport: missing fetch request payload")
	}

	body, err := encodeFetchRequest(*env.FetchRequest)
	if err != nil {
		return err
	}
	pendingBytes := int64(len(body))
	s.trackPending(pendingBytes)
	pendingReleased := false
	defer func() {
		if !pendingReleased {
			s.releasePending(pendingBytes)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), s.adapter.rpcTimeout)
	defer cancel()

	// V1 intentionally serializes one in-flight fetch RPC per peer, so a single
	// stable shard key is enough for this path.
	respBody, err := s.adapter.client.RPCService(ctx, uint64(s.peer), fetchRPCShardKey, RPCServiceFetch, body)
	if err != nil {
		return err
	}
	resp, err := decodeFetchResponse(respBody)
	if err != nil {
		return err
	}
	s.releasePending(pendingBytes)
	pendingReleased = true

	s.adapter.deliver(isrnode.Envelope{
		Peer:          s.peer,
		GroupKey:      resp.GroupKey,
		Epoch:         resp.Epoch,
		Generation:    resp.Generation,
		RequestID:     env.RequestID,
		Kind:          isrnode.MessageKindFetchResponse,
		FetchResponse: &resp,
	})
	return nil
}

func (s *peerSession) TryBatch(env isrnode.Envelope) bool {
	return false
}

func (s *peerSession) Flush() error {
	return nil
}

func (s *peerSession) Backpressure() isrnode.BackpressureState {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := isrnode.BackpressureState{
		PendingRequests: s.pendingRequests,
		PendingBytes:    s.pendingBytes,
	}
	if s.pendingRequests > 0 {
		state.Level = isrnode.BackpressureHard
	}
	return state
}

func (s *peerSession) Close() error {
	return nil
}

func (s *peerSession) trackPending(bytes int64) {
	s.mu.Lock()
	s.pendingRequests++
	s.pendingBytes += bytes
	s.mu.Unlock()
}

func (s *peerSession) releasePending(bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pendingRequests > 0 {
		s.pendingRequests--
	}
	s.pendingBytes -= bytes
	if s.pendingBytes < 0 {
		s.pendingBytes = 0
	}
}
