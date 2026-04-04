package isrnode

import (
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

func TestMaxFetchInflightPeerQueuesExcessReplication(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.Limits.MaxFetchInflightPeer = 1
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(51, 1, 1, []isr.NodeID{1, 2}))
	mustEnsureLocal(t, env.runtime, testMetaLocal(52, 1, 1, []isr.NodeID{1, 2}))

	env.runtime.enqueueReplication(testGroupKey(51), 2)
	env.runtime.enqueueReplication(testGroupKey(52), 2)
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

	env.runtime.enqueueReplication(testGroupKey(71), 2)
	env.runtime.enqueueReplication(testGroupKey(72), 2)
	env.runtime.runScheduler()
	if got := env.runtime.queuedPeerRequests(2); got != 1 {
		t.Fatalf("expected queued request before response, got %d", got)
	}

	env.transport.deliver(Envelope{
		Peer:       2,
		GroupKey:   testGroupKey(71),
		Generation: 1,
		Epoch:      3,
		Kind:       MessageKindFetchResponse,
		FetchResponse: &FetchResponseEnvelope{
			LeaderHW: 9,
			Records:  []isr.Record{{Payload: []byte("r1"), SizeBytes: 2}},
		},
	})

	if got := env.sessions.session(2).sendCount(); got != 2 {
		t.Fatalf("expected queued send to drain after response, got %d sends", got)
	}
	if got := env.runtime.queuedPeerRequests(2); got != 0 {
		t.Fatalf("expected peer queue drained, got %d", got)
	}
}

func TestFetchResponseApplyErrorKeepsPeerInflightAndQueuedReplication(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.Limits.MaxFetchInflightPeer = 1
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(81, 3, 1, []isr.NodeID{1, 2}))
	mustEnsureLocal(t, env.runtime, testMetaLocal(82, 3, 1, []isr.NodeID{1, 2}))
	mustEnsureLocal(t, env.runtime, testMetaLocal(83, 3, 1, []isr.NodeID{1, 2}))

	env.factory.replicas[0].applyFetchErr = isr.ErrCorruptState

	env.runtime.enqueueReplication(testGroupKey(81), 2)
	env.runtime.enqueueReplication(testGroupKey(82), 2)
	env.runtime.runScheduler()
	if got := env.runtime.queuedPeerRequests(2); got != 1 {
		t.Fatalf("expected queued request before failed response, got %d", got)
	}

	env.transport.deliver(Envelope{
		Peer:       2,
		GroupKey:   testGroupKey(81),
		Generation: 1,
		Epoch:      3,
		Kind:       MessageKindFetchResponse,
		FetchResponse: &FetchResponseEnvelope{
			LeaderHW: 9,
			Records:  []isr.Record{{Payload: []byte("r1"), SizeBytes: 2}},
		},
	})

	if got := env.sessions.session(2).sendCount(); got != 1 {
		t.Fatalf("apply failure should not drain queued send, got %d sends", got)
	}
	if got := env.runtime.queuedPeerRequests(2); got != 1 {
		t.Fatalf("apply failure should keep queued request, got %d", got)
	}

	env.runtime.enqueueReplication(testGroupKey(83), 2)
	env.runtime.runScheduler()

	if got := env.sessions.session(2).sendCount(); got != 1 {
		t.Fatalf("apply failure should keep peer inflight, got %d sends", got)
	}
	if got := env.runtime.queuedPeerRequests(2); got != 2 {
		t.Fatalf("expected later replication to remain queued, got %d", got)
	}
}

func TestDrainPeerQueueSendErrorRequeuesReplication(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.Limits.MaxFetchInflightPeer = 1
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(84, 3, 1, []isr.NodeID{1, 2}))
	mustEnsureLocal(t, env.runtime, testMetaLocal(85, 3, 1, []isr.NodeID{1, 2}))

	env.runtime.enqueueReplication(testGroupKey(84), 2)
	env.runtime.enqueueReplication(testGroupKey(85), 2)
	env.runtime.runScheduler()

	session := env.sessions.session(2)
	session.setSendErr(errors.New("boom"))
	env.transport.deliver(Envelope{
		Peer:       2,
		GroupKey:   testGroupKey(84),
		Generation: 1,
		Epoch:      3,
		Kind:       MessageKindFetchResponse,
		FetchResponse: &FetchResponseEnvelope{
			LeaderHW: 9,
			Records:  []isr.Record{{Payload: []byte("r1"), SizeBytes: 2}},
		},
	})

	session.setSendErr(nil)
	env.runtime.runScheduler()

	if got := session.sendCount(); got != 3 {
		t.Fatalf("expected queued replication to retry after drain send error, got %d sends", got)
	}
	if got := env.runtime.queuedPeerRequests(2); got != 0 {
		t.Fatalf("expected peer queue drained after retry, got %d", got)
	}
}

func TestUnknownFetchResponseDoesNotReleasePeerInflight(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.Limits.MaxFetchInflightPeer = 1
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(73, 3, 1, []isr.NodeID{1, 2}))
	mustEnsureLocal(t, env.runtime, testMetaLocal(74, 3, 1, []isr.NodeID{1, 2}))

	env.runtime.enqueueReplication(testGroupKey(73), 2)
	env.runtime.enqueueReplication(testGroupKey(74), 2)
	env.runtime.runScheduler()

	env.transport.deliver(Envelope{
		Peer:       2,
		GroupKey:   testGroupKey(999),
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

func TestHardBackpressureQueuesWithoutSending(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.Limits.MaxFetchInflightPeer = 2
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(75, 3, 1, []isr.NodeID{1, 2}))

	env.sessions.session(2).setBackpressure(BackpressureState{Level: BackpressureHard})
	env.runtime.enqueueReplication(testGroupKey(75), 2)
	env.runtime.runScheduler()

	if got := env.sessions.session(2).sendCount(); got != 0 {
		t.Fatalf("hard backpressure should block immediate send, got %d", got)
	}
	if got := env.runtime.queuedPeerRequests(2); got != 1 {
		t.Fatalf("hard backpressure should queue request, got %d", got)
	}
}

func TestPeerRequestStateRetainsQueueBucketAfterDrain(t *testing.T) {
	state := newPeerRequestState()
	peer := isr.NodeID(2)

	state.enqueue(Envelope{Peer: peer, GroupKey: testGroupKey(1)})
	state.enqueue(Envelope{Peer: peer, GroupKey: testGroupKey(2)})

	if _, ok := state.popQueued(peer); !ok {
		t.Fatal("expected first queued envelope")
	}
	if _, ok := state.popQueued(peer); !ok {
		t.Fatal("expected second queued envelope")
	}

	if _, ok := state.queued[peer]; !ok {
		t.Fatal("expected drained peer queue bucket to be retained for reuse")
	}
	if got := state.queuedCount(peer); got != 0 {
		t.Fatalf("queuedCount() = %d, want 0", got)
	}

	state.enqueue(Envelope{Peer: peer, GroupKey: testGroupKey(3)})
	if got := state.queuedCount(peer); got != 1 {
		t.Fatalf("queuedCount() after reuse = %d, want 1", got)
	}
}

func TestPeerQueueRetainsEnvelopeGroupKeyOrder(t *testing.T) {
	state := newPeerRequestState()
	peer := isr.NodeID(2)

	state.enqueue(Envelope{Peer: peer, GroupKey: testGroupKey(1)})
	state.enqueue(Envelope{Peer: peer, GroupKey: testGroupKey(2)})
	state.enqueue(Envelope{Peer: peer, GroupKey: testGroupKey(3)})

	for _, want := range []uint64{1, 2} {
		env, ok := state.popQueued(peer)
		if !ok {
			t.Fatalf("popQueued() missing envelope %d", want)
		}
		if env.GroupKey != testGroupKey(want) {
			t.Fatalf("popQueued() group = %q, want %q", env.GroupKey, testGroupKey(want))
		}
	}

	state.enqueue(Envelope{Peer: peer, GroupKey: testGroupKey(4)})

	for _, want := range []uint64{3, 4} {
		env, ok := state.popQueued(peer)
		if !ok {
			t.Fatalf("popQueued() missing envelope %d", want)
		}
		if env.GroupKey != testGroupKey(want) {
			t.Fatalf("popQueued() group = %q, want %q", env.GroupKey, testGroupKey(want))
		}
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
