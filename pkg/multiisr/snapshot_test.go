package multiisr

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/isr"
)

func TestSnapshotTasksRespectMaxSnapshotInflight(t *testing.T) {
	env := newSnapshotTestEnv(t, func(cfg *Config) {
		cfg.Limits.MaxSnapshotInflight = 1
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(61, 1, 1, []isr.NodeID{1, 2}))
	mustEnsureLocal(t, env.runtime, testMetaLocal(62, 1, 1, []isr.NodeID{1, 2}))

	env.runtime.queueSnapshot(61)
	env.runtime.queueSnapshot(62)
	env.runtime.runScheduler()

	if got := env.runtime.maxSnapshotConcurrent(); got != 1 {
		t.Fatalf("expected single inflight snapshot, got %d", got)
	}
}

func TestRecoveryBandwidthLimiterThrottlesSnapshotChunks(t *testing.T) {
	env := newSnapshotTestEnv(t, func(cfg *Config) {
		cfg.Limits.MaxRecoveryBytesPerSecond = 128
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(63, 1, 1, []isr.NodeID{1, 2}))

	startedAt := env.clock.Now()
	env.runtime.queueSnapshotChunk(63, 256)
	env.runtime.runScheduler()
	if env.clock.Now().Sub(startedAt) < time.Second {
		t.Fatalf("expected throttling delay")
	}
}

func TestSnapshotTaskRequeuesWhenInflightLimitReached(t *testing.T) {
	env := newSnapshotTestEnv(t, func(cfg *Config) {
		cfg.Limits.MaxSnapshotInflight = 1
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(64, 1, 1, []isr.NodeID{1, 2}))
	mustEnsureLocal(t, env.runtime, testMetaLocal(65, 1, 1, []isr.NodeID{1, 2}))

	env.runtime.snapshotRunner = func(groupID uint64, bytes int64) bool {
		return groupID != 64
	}
	env.runtime.queueSnapshot(64)
	env.runtime.queueSnapshot(65)
	env.runtime.runScheduler()
	if got := env.runtime.queuedSnapshotGroups(); got != 1 {
		t.Fatalf("expected one waiting snapshot, got %d", got)
	}

	env.runtime.completeSnapshot(64)
	env.runtime.runScheduler()
	if got := env.runtime.queuedSnapshotGroups(); got != 0 {
		t.Fatalf("expected waiting snapshot to be resumed, got %d", got)
	}
}

type snapshotTestEnv struct {
	runtime *runtime
	clock   *snapshotManualClock
}

func newSnapshotTestEnv(t *testing.T, mutate func(*Config)) *snapshotTestEnv {
	t.Helper()

	clock := newSnapshotManualClock(time.Unix(1700000000, 0))
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
		Now: clock.Now,
	}
	if mutate != nil {
		mutate(&cfg)
	}

	rt, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	impl := rt.(*runtime)
	impl.advanceClock = clock.Advance

	return &snapshotTestEnv{
		runtime: impl,
		clock:   clock,
	}
}

type snapshotManualClock struct {
	now time.Time
}

func newSnapshotManualClock(now time.Time) *snapshotManualClock {
	return &snapshotManualClock{now: now}
}

func (c *snapshotManualClock) Now() time.Time {
	return c.now
}

func (c *snapshotManualClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}
