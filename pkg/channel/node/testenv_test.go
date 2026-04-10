package node_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
	isrnode "github.com/WuKongIM/WuKongIM/pkg/channel/node"
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

func withMaxChannels(n int) testEnvOption {
	return func(cfg *isrnode.Config) {
		cfg.Limits.MaxChannels = n
	}
}

func testChannelKey(groupID uint64) isr.ChannelKey {
	return isr.ChannelKey("group-" + strconv.FormatUint(groupID, 10))
}

func testMeta(groupID, epoch uint64, leader isr.NodeID, replicas []isr.NodeID) isr.ChannelMeta {
	return isr.ChannelMeta{
		ChannelKey: testChannelKey(groupID),
		Epoch:      epoch,
		Leader:     leader,
		Replicas:   append([]isr.NodeID(nil), replicas...),
		ISR:        append([]isr.NodeID(nil), replicas...),
		MinISR:     1,
	}
}

func mustEnsure(t *testing.T, rt isrnode.Runtime, meta isr.ChannelMeta) {
	t.Helper()
	if err := rt.EnsureChannel(meta); err != nil {
		t.Fatalf("EnsureChannel(%q) error = %v", meta.ChannelKey, err)
	}
}

func mustRemove(t *testing.T, rt isrnode.Runtime, groupID uint64) {
	t.Helper()
	channelKey := testChannelKey(groupID)
	if err := rt.RemoveChannel(channelKey); err != nil {
		t.Fatalf("RemoveChannel(%q) error = %v", channelKey, err)
	}
}

func mustChannel(t *testing.T, rt isrnode.Runtime, groupID uint64) isrnode.ChannelHandle {
	t.Helper()
	channelKey := testChannelKey(groupID)
	group, ok := rt.Channel(channelKey)
	if !ok {
		t.Fatalf("Channel(%q) not found", channelKey)
	}
	return group
}

func fencedLeaderMeta(groupID uint64) isr.ChannelMeta {
	meta := testMeta(groupID, 1, 1, []isr.NodeID{1, 2})
	meta.LeaseUntil = time.Unix(1699999999, 0)
	return meta
}

type fakeGenerationStore struct {
	mu     sync.Mutex
	values map[isr.ChannelKey]uint64
	stored map[isr.ChannelKey]uint64
}

func newFakeGenerationStore() *fakeGenerationStore {
	return &fakeGenerationStore{
		values: make(map[isr.ChannelKey]uint64),
		stored: make(map[isr.ChannelKey]uint64),
	}
}

func (s *fakeGenerationStore) Load(channelKey isr.ChannelKey) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[channelKey], nil
}

func (s *fakeGenerationStore) Store(channelKey isr.ChannelKey, generation uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[channelKey] = generation
	s.stored[channelKey] = generation
	return nil
}

type createdReplica struct {
	channelKey isr.ChannelKey
	generation uint64
	meta       isr.ChannelMeta
}

type fakeReplicaFactory struct {
	mu       sync.Mutex
	created  []createdReplica
	replicas []*fakeReplica
}

func newFakeReplicaFactory() *fakeReplicaFactory {
	return &fakeReplicaFactory{}
}

func (f *fakeReplicaFactory) New(cfg isrnode.ChannelConfig) (isr.Replica, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	replica := &fakeReplica{
		state: isr.ReplicaState{
			ChannelKey: cfg.ChannelKey,
			Epoch:      cfg.Meta.Epoch,
			Leader:     cfg.Meta.Leader,
			Role:       isr.RoleFollower,
		},
	}
	f.created = append(f.created, createdReplica{
		channelKey: cfg.ChannelKey,
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
	fetchErr        error
	fetchResult     isr.FetchResult
	metaCalls       []isr.ChannelMeta
	appendCalls     int
	applyFetchCalls int
	fetchCalls      int
	tombstoned      bool
}

func (r *fakeReplica) ApplyMeta(meta isr.ChannelMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metaCalls = append(r.metaCalls, meta)
	r.state.ChannelKey = meta.ChannelKey
	r.state.Epoch = meta.Epoch
	r.state.Leader = meta.Leader
	return nil
}

func (r *fakeReplica) BecomeLeader(meta isr.ChannelMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metaCalls = append(r.metaCalls, meta)
	r.state.ChannelKey = meta.ChannelKey
	r.state.Epoch = meta.Epoch
	r.state.Leader = meta.Leader
	r.state.Role = isr.RoleLeader
	return nil
}

func (r *fakeReplica) BecomeFollower(meta isr.ChannelMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metaCalls = append(r.metaCalls, meta)
	r.state.ChannelKey = meta.ChannelKey
	r.state.Epoch = meta.Epoch
	r.state.Leader = meta.Leader
	r.state.Role = isr.RoleFollower
	return nil
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
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fetchCalls++
	return r.fetchResult, r.fetchErr
}

func (r *fakeReplica) ApplyFetch(ctx context.Context, req isr.ApplyFetchRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applyFetchCalls++
	return nil
}

func (r *fakeReplica) ApplyProgressAck(ctx context.Context, req isr.ProgressAckRequest) error {
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
