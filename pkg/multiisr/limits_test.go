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
