package runtime

import (
	"testing"
	"time"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
)

func TestSnapshotTasksRespectMaxSnapshotInflight(t *testing.T) {
	env := newSnapshotTestEnv(t, func(cfg *Config) {
		cfg.Limits.MaxSnapshotInflight = 1
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(61, 1, 1, []core.NodeID{1, 2}))
	mustEnsureLocal(t, env.runtime, testMetaLocal(62, 1, 1, []core.NodeID{1, 2}))

	env.runtime.queueSnapshot(testChannelKey(61))
	env.runtime.queueSnapshot(testChannelKey(62))
	env.runtime.runScheduler()

	if got := env.runtime.maxSnapshotConcurrent(); got != 1 {
		t.Fatalf("expected single inflight snapshot, got %d", got)
	}
}

func TestSnapshotRecoveryBandwidthLimiterThrottlesSnapshotChunks(t *testing.T) {
	env := newSnapshotTestEnv(t, func(cfg *Config) {
		cfg.Limits.MaxRecoveryBytesPerSecond = 128
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(63, 1, 1, []core.NodeID{1, 2}))

	startedAt := env.clock.Now()
	env.runtime.queueSnapshotChunk(testChannelKey(63), 256)
	env.runtime.runScheduler()
	if env.clock.Now().Sub(startedAt) < time.Second {
		t.Fatalf("expected throttling delay")
	}
}

func TestSnapshotTaskRequeuesWhenInflightLimitReached(t *testing.T) {
	env := newSnapshotTestEnv(t, func(cfg *Config) {
		cfg.Limits.MaxSnapshotInflight = 1
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(64, 1, 1, []core.NodeID{1, 2}))
	mustEnsureLocal(t, env.runtime, testMetaLocal(65, 1, 1, []core.NodeID{1, 2}))

	if !env.runtime.snapshots.begin(1) {
		t.Fatalf("expected to reserve one inflight snapshot")
	}
	env.runtime.queueSnapshot(testChannelKey(64))
	env.runtime.queueSnapshot(testChannelKey(65))
	env.runtime.runScheduler()
	if got := env.runtime.queuedSnapshotGroups(); got != 2 {
		t.Fatalf("expected two waiting snapshots, got %d", got)
	}

	env.runtime.completeSnapshot("")
	env.runtime.runScheduler()
	if got := env.runtime.queuedSnapshotGroups(); got != 0 {
		t.Fatalf("expected waiting snapshot to be resumed, got %d", got)
	}
}

func TestSnapshotWaitingQueueIsFIFO(t *testing.T) {
	env := newSnapshotTestEnv(t, func(cfg *Config) {
		cfg.Limits.MaxSnapshotInflight = 1
	})
	mustEnsureLocal(t, env.runtime, testMetaLocal(66, 1, 1, []core.NodeID{1, 2}))
	mustEnsureLocal(t, env.runtime, testMetaLocal(67, 1, 1, []core.NodeID{1, 2}))
	mustEnsureLocal(t, env.runtime, testMetaLocal(68, 1, 1, []core.NodeID{1, 2}))

	if !env.runtime.snapshots.begin(1) {
		t.Fatalf("expected to reserve one inflight snapshot")
	}
	env.runtime.queueSnapshotChunk(testChannelKey(66), 101)
	env.runtime.queueSnapshotChunk(testChannelKey(67), 102)
	env.runtime.queueSnapshotChunk(testChannelKey(68), 103)
	env.runtime.runScheduler()

	env.runtime.completeSnapshot("")
	env.runtime.runScheduler()

	if len(env.throttle.values) != 3 || env.throttle.values[0] != 101 || env.throttle.values[1] != 102 || env.throttle.values[2] != 103 {
		t.Fatalf("expected FIFO throttle trace [101 102 103], got %v", env.throttle.values)
	}
}

type snapshotTestEnv struct {
	runtime  *runtime
	clock    *snapshotManualClock
	throttle *fakeSnapshotThrottle
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
	throttle := &fakeSnapshotThrottle{advance: clock.Advance, rate: cfg.Limits.MaxRecoveryBytesPerSecond}
	impl.snapshotThrottle = throttle

	return &snapshotTestEnv{
		runtime:  impl,
		clock:    clock,
		throttle: throttle,
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

type fakeSnapshotThrottle struct {
	advance func(time.Duration)
	values  []int64
	rate    int64
}

func (t *fakeSnapshotThrottle) Wait(bytes int64) {
	t.values = append(t.values, bytes)
	if t.advance != nil && bytes > 0 {
		rate := t.rate
		if rate <= 0 {
			return
		}
		t.advance(time.Duration(bytes) * time.Second / time.Duration(rate))
	}
}
