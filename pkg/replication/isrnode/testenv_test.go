package isrnode_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isrnode"
)

type testEnv struct {
	runtime     isrnode.Runtime
	generations *fakeGenerationStore
	factory     *fakeReplicaFactory
	transport   *fakeTransport
	sessions    *fakePeerSessionManager
	clock       *manualClock
}

func newTestEnv(t *testing.T) *testEnv {
	return newTestEnvWithOptions(t)
}

type testEnvOption func(*isrnode.Config)

func newTestEnvWithOptions(t *testing.T, opts ...testEnvOption) *testEnv {
	t.Helper()

	clock := newManualClock(time.Unix(1700000000, 0))
	generations := newFakeGenerationStore()
	factory := newFakeReplicaFactory()
	transport := &fakeTransport{}
	sessions := &fakePeerSessionManager{}

	cfg := isrnode.Config{
		LocalNode:       1,
		ReplicaFactory:  factory,
		GenerationStore: generations,
		Transport:       transport,
		PeerSessions:    sessions,
		Tombstones: isrnode.TombstonePolicy{
			TombstoneTTL: 30 * time.Second,
		},
		Now: clock.Now,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	rt, err := isrnode.New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	return &testEnv{
		runtime:     rt,
		generations: generations,
		factory:     factory,
		transport:   transport,
		sessions:    sessions,
		clock:       clock,
	}
}

func withMaxGroups(n int) testEnvOption {
	return func(cfg *isrnode.Config) {
		cfg.Limits.MaxGroups = n
	}
}

func testMeta(groupID, epoch uint64, leader isr.NodeID, replicas []isr.NodeID) isr.GroupMeta {
	return isr.GroupMeta{
		GroupID:  groupID,
		Epoch:    epoch,
		Leader:   leader,
		Replicas: append([]isr.NodeID(nil), replicas...),
		ISR:      append([]isr.NodeID(nil), replicas...),
		MinISR:   1,
	}
}

func mustEnsure(t *testing.T, rt isrnode.Runtime, meta isr.GroupMeta) {
	t.Helper()
	if err := rt.EnsureGroup(meta); err != nil {
		t.Fatalf("EnsureGroup(%d) error = %v", meta.GroupID, err)
	}
}

func mustRemove(t *testing.T, rt isrnode.Runtime, groupID uint64) {
	t.Helper()
	if err := rt.RemoveGroup(groupID); err != nil {
		t.Fatalf("RemoveGroup(%d) error = %v", groupID, err)
	}
}

func mustGroup(t *testing.T, rt isrnode.Runtime, groupID uint64) isrnode.GroupHandle {
	t.Helper()
	group, ok := rt.Group(groupID)
	if !ok {
		t.Fatalf("Group(%d) not found", groupID)
	}
	return group
}

func fencedLeaderMeta(groupID uint64) isr.GroupMeta {
	meta := testMeta(groupID, 1, 1, []isr.NodeID{1, 2})
	meta.LeaseUntil = time.Unix(1699999999, 0)
	return meta
}

type fakeGenerationStore struct {
	mu     sync.Mutex
	values map[uint64]uint64
	stored map[uint64]uint64
}

func newFakeGenerationStore() *fakeGenerationStore {
	return &fakeGenerationStore{
		values: make(map[uint64]uint64),
		stored: make(map[uint64]uint64),
	}
}

func (s *fakeGenerationStore) Load(groupID uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[groupID], nil
}

func (s *fakeGenerationStore) Store(groupID uint64, generation uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[groupID] = generation
	s.stored[groupID] = generation
	return nil
}

type createdReplica struct {
	groupID    uint64
	generation uint64
	meta       isr.GroupMeta
}

type fakeReplicaFactory struct {
	mu       sync.Mutex
	created  []createdReplica
	replicas []*fakeReplica
}

func newFakeReplicaFactory() *fakeReplicaFactory {
	return &fakeReplicaFactory{}
}

func (f *fakeReplicaFactory) New(cfg isrnode.GroupConfig) (isr.Replica, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	replica := &fakeReplica{
		state: isr.ReplicaState{
			GroupID: cfg.GroupID,
			Epoch:   cfg.Meta.Epoch,
			Leader:  cfg.Meta.Leader,
			Role:    isr.RoleFollower,
		},
	}
	f.created = append(f.created, createdReplica{
		groupID:    cfg.GroupID,
		generation: cfg.Generation,
		meta:       cfg.Meta,
	})
	f.replicas = append(f.replicas, replica)
	return replica, nil
}

type fakeReplica struct {
	mu              sync.Mutex
	state           isr.ReplicaState
	appendErr       error
	metaCalls       []isr.GroupMeta
	appendCalls     int
	applyFetchCalls int
	tombstoned      bool
}

func (r *fakeReplica) ApplyMeta(meta isr.GroupMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metaCalls = append(r.metaCalls, meta)
	r.state.GroupID = meta.GroupID
	r.state.Epoch = meta.Epoch
	r.state.Leader = meta.Leader
	return nil
}

func (r *fakeReplica) BecomeLeader(meta isr.GroupMeta) error {
	return r.ApplyMeta(meta)
}

func (r *fakeReplica) BecomeFollower(meta isr.GroupMeta) error {
	return r.ApplyMeta(meta)
}

func (r *fakeReplica) Tombstone() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tombstoned = true
	r.state.Role = isr.RoleTombstoned
	return nil
}

func (r *fakeReplica) InstallSnapshot(ctx context.Context, snap isr.Snapshot) error {
	return nil
}

func (r *fakeReplica) Append(ctx context.Context, batch []isr.Record) (isr.CommitResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.appendCalls++
	return isr.CommitResult{}, r.appendErr
}

func (r *fakeReplica) Fetch(ctx context.Context, req isr.FetchRequest) (isr.FetchResult, error) {
	return isr.FetchResult{}, nil
}

func (r *fakeReplica) ApplyFetch(ctx context.Context, req isr.ApplyFetchRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applyFetchCalls++
	return nil
}

func (r *fakeReplica) Status() isr.ReplicaState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

type fakeTransport struct {
	mu      sync.Mutex
	handler func(isrnode.Envelope)
}

func (t *fakeTransport) Send(peer isr.NodeID, env isrnode.Envelope) error {
	return nil
}

func (t *fakeTransport) RegisterHandler(fn func(isrnode.Envelope)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = fn
}

func (t *fakeTransport) deliver(env isrnode.Envelope) {
	t.mu.Lock()
	handler := t.handler
	t.mu.Unlock()
	if handler != nil {
		handler(env)
	}
}

type fakePeerSessionManager struct{}

func (m *fakePeerSessionManager) Session(peer isr.NodeID) isrnode.PeerSession {
	return &fakePeerSession{}
}

type fakePeerSession struct{}

func (s *fakePeerSession) Send(env isrnode.Envelope) error {
	return nil
}

func (s *fakePeerSession) TryBatch(env isrnode.Envelope) bool {
	return false
}

func (s *fakePeerSession) Flush() error {
	return nil
}

func (s *fakePeerSession) Backpressure() isrnode.BackpressureState {
	return isrnode.BackpressureState{}
}

func (s *fakePeerSession) Close() error {
	return nil
}

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock(now time.Time) *manualClock {
	return &manualClock{now: now}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
