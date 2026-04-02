package multiisr

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/isr"
)

func TestMaxFetchInflightPeerQueuesExcessReplication(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.Limits.MaxFetchInflightPeer = 1
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(51, 1, 1, []isr.NodeID{1, 2}))
	mustEnsureLocal(t, env.runtime, testMetaLocal(52, 1, 1, []isr.NodeID{1, 2}))

	env.runtime.enqueueReplication(51, 2)
	env.runtime.enqueueReplication(52, 2)
	env.runtime.runScheduler()

	if got := env.sessions.session(2).sendCount(); got != 1 {
		t.Fatalf("expected one immediate send, got %d", got)
	}
	if got := env.runtime.queuedPeerRequests(2); got != 1 {
		t.Fatalf("expected one queued request, got %d", got)
	}
}

func TestFetchResponseDrainsQueuedReplicationForPeer(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.Limits.MaxFetchInflightPeer = 1
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(71, 3, 1, []isr.NodeID{1, 2}))
	mustEnsureLocal(t, env.runtime, testMetaLocal(72, 3, 1, []isr.NodeID{1, 2}))

	env.runtime.enqueueReplication(71, 2)
	env.runtime.enqueueReplication(72, 2)
	env.runtime.runScheduler()
	if got := env.runtime.queuedPeerRequests(2); got != 1 {
		t.Fatalf("expected queued request before response, got %d", got)
	}

	payload := mustEncodeFetchResponsePayload(t, fetchResponsePayload{
		LeaderHW: 9,
		Records:  []isr.Record{{Payload: []byte("r1"), SizeBytes: 2}},
	})
	env.transport.deliver(Envelope{
		Peer:       2,
		GroupID:    71,
		Generation: 1,
		Epoch:      3,
		Kind:       MessageKindFetchResponse,
		Payload:    payload,
	})

	if got := env.sessions.session(2).sendCount(); got != 2 {
		t.Fatalf("expected queued send to drain after response, got %d sends", got)
	}
	if got := env.runtime.queuedPeerRequests(2); got != 0 {
		t.Fatalf("expected peer queue drained, got %d", got)
	}
}

func TestUnknownFetchResponseDoesNotReleasePeerInflight(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.Limits.MaxFetchInflightPeer = 1
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(73, 3, 1, []isr.NodeID{1, 2}))
	mustEnsureLocal(t, env.runtime, testMetaLocal(74, 3, 1, []isr.NodeID{1, 2}))

	env.runtime.enqueueReplication(73, 2)
	env.runtime.enqueueReplication(74, 2)
	env.runtime.runScheduler()

	env.transport.deliver(Envelope{
		Peer:       2,
		GroupID:    999,
		Generation: 999,
		Epoch:      3,
		Kind:       MessageKindFetchResponse,
	})

	if got := env.sessions.session(2).sendCount(); got != 1 {
		t.Fatalf("unexpected send count after unknown response: %d", got)
	}
	if got := env.runtime.queuedPeerRequests(2); got != 1 {
		t.Fatalf("unknown response should not drain queue, got %d queued", got)
	}
}

func newSessionTestEnvWithConfig(t *testing.T, mutate func(*Config)) *sessionTestEnv {
	t.Helper()

	generations := newSessionGenerationStore()
	factory := newSessionReplicaFactory()
	transport := &sessionTransport{}
	sessions := newSessionPeerSessionManager()

	cfg := Config{
		LocalNode:       1,
		ReplicaFactory:  factory,
		GenerationStore: generations,
		Transport:       transport,
		PeerSessions:    sessions,
		Tombstones: TombstonePolicy{
			TombstoneTTL: 30 * time.Second,
		},
		Now: func() time.Time { return time.Unix(1700000000, 0) },
	}
	if mutate != nil {
		mutate(&cfg)
	}

	rt, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	return &sessionTestEnv{
		runtime:     rt.(*runtime),
		generations: generations,
		factory:     factory,
		transport:   transport,
		sessions:    sessions,
	}
}
