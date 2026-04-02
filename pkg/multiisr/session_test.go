package multiisr

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/isr"
)

func TestManyGroupsToSamePeerReuseOneSession(t *testing.T) {
	env := newSessionTestEnv(t)
	mustEnsureLocal(t, env.runtime, testMetaLocal(21, 1, 1, []isr.NodeID{1, 2}))
	mustEnsureLocal(t, env.runtime, testMetaLocal(22, 1, 1, []isr.NodeID{1, 2}))

	env.runtime.enqueueReplication(21, 2)
	env.runtime.enqueueReplication(22, 2)
	env.runtime.runScheduler()

	if got := env.sessions.createdFor(2); got != 1 {
		t.Fatalf("expected one session for peer 2, got %d", got)
	}
}

func TestInboundEnvelopeDemuxRequiresMatchingGeneration(t *testing.T) {
	env := newSessionTestEnv(t)
	mustEnsureLocal(t, env.runtime, testMetaLocal(23, 1, 1, []isr.NodeID{1, 2}))

	env.transport.deliver(Envelope{GroupID: 23, Generation: 99, Epoch: 1, Kind: MessageKindFetchResponse})
	if env.factory.replicas[0].applyFetchCalls != 0 {
		t.Fatalf("unexpected apply fetch on generation mismatch")
	}
}

func TestFetchResponseDropsStaleEpoch(t *testing.T) {
	env := newSessionTestEnv(t)
	mustEnsureLocal(t, env.runtime, testMetaLocal(24, 3, 1, []isr.NodeID{1, 2}))

	payload := mustEncodeFetchResponsePayload(t, fetchResponsePayload{
		LeaderHW: 5,
		Records:  []isr.Record{{Payload: []byte("stale"), SizeBytes: 5}},
	})
	env.transport.deliver(Envelope{
		Peer:       2,
		GroupID:    24,
		Generation: 1,
		Epoch:      2,
		Kind:       MessageKindFetchResponse,
		Payload:    payload,
	})
	if env.factory.replicas[0].applyFetchCalls != 0 {
		t.Fatalf("stale epoch response should be dropped")
	}
}

func TestFetchResponseDecodesPayloadIntoApplyFetch(t *testing.T) {
	env := newSessionTestEnv(t)
	mustEnsureLocal(t, env.runtime, testMetaLocal(25, 4, 1, []isr.NodeID{1, 2}))

	truncateTo := uint64(7)
	payload := mustEncodeFetchResponsePayload(t, fetchResponsePayload{
		LeaderHW:   11,
		TruncateTo: &truncateTo,
		Records:    []isr.Record{{Payload: []byte("ok"), SizeBytes: 2}},
	})
	env.transport.deliver(Envelope{
		Peer:       2,
		GroupID:    25,
		Generation: 1,
		Epoch:      4,
		Kind:       MessageKindFetchResponse,
		Payload:    payload,
	})

	if env.factory.replicas[0].applyFetchCalls != 1 {
		t.Fatalf("expected fetch response to be applied")
	}
	got := env.factory.replicas[0].lastApplyFetch
	if got.LeaderHW != 11 {
		t.Fatalf("expected LeaderHW 11, got %d", got.LeaderHW)
	}
	if got.TruncateTo == nil || *got.TruncateTo != 7 {
		t.Fatalf("expected TruncateTo 7, got %+v", got.TruncateTo)
	}
	if len(got.Records) != 1 || string(got.Records[0].Payload) != "ok" {
		t.Fatalf("unexpected records: %+v", got.Records)
	}
}

type sessionTestEnv struct {
	runtime     *runtime
	generations *sessionGenerationStore
	factory     *sessionReplicaFactory
	transport   *sessionTransport
	sessions    *sessionPeerSessionManager
}

func newSessionTestEnv(t *testing.T) *sessionTestEnv {
	t.Helper()

	generations := newSessionGenerationStore()
	factory := newSessionReplicaFactory()
	transport := &sessionTransport{}
	sessions := newSessionPeerSessionManager()

	rt, err := New(Config{
		LocalNode:       1,
		ReplicaFactory:  factory,
		GenerationStore: generations,
		Transport:       transport,
		PeerSessions:    sessions,
		Tombstones: TombstonePolicy{
			TombstoneTTL: 30 * time.Second,
		},
		Now: func() time.Time { return time.Unix(1700000000, 0) },
	})
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

func testMetaLocal(groupID, epoch uint64, leader isr.NodeID, replicas []isr.NodeID) isr.GroupMeta {
	return isr.GroupMeta{
		GroupID:  groupID,
		Epoch:    epoch,
		Leader:   leader,
		Replicas: append([]isr.NodeID(nil), replicas...),
		ISR:      append([]isr.NodeID(nil), replicas...),
		MinISR:   1,
	}
}

func mustEnsureLocal(t *testing.T, rt *runtime, meta isr.GroupMeta) {
	t.Helper()
	if err := rt.EnsureGroup(meta); err != nil {
		t.Fatalf("EnsureGroup(%d) error = %v", meta.GroupID, err)
	}
}

type sessionGenerationStore struct {
	mu     sync.Mutex
	values map[uint64]uint64
}

func newSessionGenerationStore() *sessionGenerationStore {
	return &sessionGenerationStore{values: make(map[uint64]uint64)}
}

func (s *sessionGenerationStore) Load(groupID uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[groupID], nil
}

func (s *sessionGenerationStore) Store(groupID uint64, generation uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[groupID] = generation
	return nil
}

type sessionReplicaFactory struct {
	mu       sync.Mutex
	replicas []*sessionReplica
}

func newSessionReplicaFactory() *sessionReplicaFactory {
	return &sessionReplicaFactory{}
}

func (f *sessionReplicaFactory) New(cfg GroupConfig) (isr.Replica, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	replica := &sessionReplica{
		state: isr.ReplicaState{
			GroupID: cfg.GroupID,
			Role:    isr.RoleLeader,
			Epoch:   cfg.Meta.Epoch,
			Leader:  cfg.Meta.Leader,
		},
	}
	f.replicas = append(f.replicas, replica)
	return replica, nil
}

type sessionReplica struct {
	mu              sync.Mutex
	state           isr.ReplicaState
	applyFetchCalls int
	lastApplyFetch  isr.ApplyFetchRequest
}

func (r *sessionReplica) ApplyMeta(meta isr.GroupMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Epoch = meta.Epoch
	r.state.Leader = meta.Leader
	return nil
}

func (r *sessionReplica) BecomeLeader(meta isr.GroupMeta) error   { return r.ApplyMeta(meta) }
func (r *sessionReplica) BecomeFollower(meta isr.GroupMeta) error { return r.ApplyMeta(meta) }
func (r *sessionReplica) Tombstone() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state.Role = isr.RoleTombstoned
	return nil
}
func (r *sessionReplica) InstallSnapshot(ctx context.Context, snap isr.Snapshot) error {
	return nil
}
func (r *sessionReplica) Append(ctx context.Context, batch []isr.Record) (isr.CommitResult, error) {
	return isr.CommitResult{}, nil
}
func (r *sessionReplica) Fetch(ctx context.Context, req isr.FetchRequest) (isr.FetchResult, error) {
	return isr.FetchResult{}, nil
}
func (r *sessionReplica) ApplyFetch(ctx context.Context, req isr.ApplyFetchRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applyFetchCalls++
	r.lastApplyFetch = req
	return nil
}
func (r *sessionReplica) Status() isr.ReplicaState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

type sessionTransport struct {
	mu      sync.Mutex
	handler func(Envelope)
}

func (t *sessionTransport) Send(peer isr.NodeID, env Envelope) error {
	return nil
}

func (t *sessionTransport) RegisterHandler(fn func(Envelope)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = fn
}

func (t *sessionTransport) deliver(env Envelope) {
	t.mu.Lock()
	handler := t.handler
	t.mu.Unlock()
	if handler != nil {
		handler(env)
	}
}

type sessionPeerSessionManager struct {
	mu      sync.Mutex
	created map[isr.NodeID]int
	cache   map[isr.NodeID]*trackingPeerSession
}

func newSessionPeerSessionManager() *sessionPeerSessionManager {
	return &sessionPeerSessionManager{
		created: make(map[isr.NodeID]int),
		cache:   make(map[isr.NodeID]*trackingPeerSession),
	}
}

func (m *sessionPeerSessionManager) Session(peer isr.NodeID) PeerSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.created[peer]++
	if session, ok := m.cache[peer]; ok {
		return session
	}
	session := &trackingPeerSession{}
	m.cache[peer] = session
	return session
}

func (m *sessionPeerSessionManager) createdFor(peer isr.NodeID) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.created[peer]
}

func (m *sessionPeerSessionManager) session(peer isr.NodeID) *trackingPeerSession {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cache[peer]
}

type trackingPeerSession struct {
	mu    sync.Mutex
	sends int
}

func (s *trackingPeerSession) Send(env Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sends++
	return nil
}

func (s *trackingPeerSession) TryBatch(env Envelope) bool {
	return false
}

func (s *trackingPeerSession) Flush() error {
	return nil
}

func (s *trackingPeerSession) Backpressure() BackpressureState {
	return BackpressureState{}
}

func (s *trackingPeerSession) Close() error {
	return nil
}

func (s *trackingPeerSession) sendCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sends
}

func mustEncodeFetchResponsePayload(t *testing.T, payload fetchResponsePayload) []byte {
	t.Helper()
	data, err := encodeFetchResponsePayload(payload)
	if err != nil {
		t.Fatalf("encodeFetchResponsePayload() error = %v", err)
	}
	return data
}
