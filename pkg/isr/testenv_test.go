package isr

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeLogStore struct {
	mu            sync.Mutex
	records       []Record
	leo           uint64
	syncCount     int
	truncateCalls []uint64
	calls         *callLog
	appendSignal  chan uint64
}

func (f *fakeLogStore) LEO() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leo
}

func (f *fakeLogStore) Append(records []Record) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	base := f.leo
	for _, record := range records {
		f.records = append(f.records, cloneRecord(record))
		f.leo++
	}
	if f.appendSignal != nil {
		select {
		case f.appendSignal <- f.leo:
		default:
		}
	}
	return base, nil
}

func (f *fakeLogStore) Read(from uint64, maxBytes int) ([]Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if from >= uint64(len(f.records)) {
		return nil, nil
	}

	var (
		total int
		out   []Record
	)
	for i := from; i < uint64(len(f.records)); i++ {
		record := cloneRecord(f.records[i])
		if len(out) > 0 && total+record.SizeBytes > maxBytes {
			break
		}
		out = append(out, record)
		total += record.SizeBytes
		if len(out) == 1 && total > maxBytes {
			break
		}
	}
	return out, nil
}

func (f *fakeLogStore) Truncate(to uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.truncateCalls = append(f.truncateCalls, to)
	if f.calls != nil {
		f.calls.add(fmt.Sprintf("log.truncate:%d", to))
	}
	if to < uint64(len(f.records)) {
		f.records = append([]Record(nil), f.records[:to]...)
	} else if to == 0 {
		f.records = nil
	}
	f.leo = to
	return nil
}

func (f *fakeLogStore) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncCount++
	if f.calls != nil {
		f.calls.add("log.sync")
	}
	return nil
}

type fakeCheckpointStore struct {
	mu         sync.Mutex
	checkpoint Checkpoint
	loadErr    error
	stored     []Checkpoint
	calls      *callLog
}

func (f *fakeCheckpointStore) Load() (Checkpoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.checkpoint, f.loadErr
}

func (f *fakeCheckpointStore) Store(checkpoint Checkpoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checkpoint = checkpoint
	f.stored = append(f.stored, checkpoint)
	if f.calls != nil {
		f.calls.add(fmt.Sprintf("checkpoint.store:%d", checkpoint.HW))
	}
	return nil
}

func (f *fakeCheckpointStore) lastStored() Checkpoint {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.stored) == 0 {
		return Checkpoint{}
	}
	return f.stored[len(f.stored)-1]
}

type fakeEpochHistoryStore struct {
	mu       sync.Mutex
	points   []EpochPoint
	loadErr  error
	appended []EpochPoint
	calls    *callLog
}

func (f *fakeEpochHistoryStore) Load() ([]EpochPoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]EpochPoint(nil), f.points...), f.loadErr
}

func (f *fakeEpochHistoryStore) Append(point EpochPoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.points) > 0 {
		last := f.points[len(f.points)-1]
		switch {
		case point.Epoch > last.Epoch:
			f.points = append(f.points, point)
		case point.Epoch == last.Epoch && point.StartOffset == last.StartOffset:
			// Idempotent replay.
		default:
			return ErrCorruptState
		}
	} else {
		f.points = append(f.points, point)
	}
	f.appended = append(f.appended, point)
	if f.calls != nil {
		f.calls.add(fmt.Sprintf("history.append:%d@%d", point.Epoch, point.StartOffset))
	}
	return nil
}

type fakeSnapshotApplier struct {
	mu        sync.Mutex
	installed []Snapshot
	calls     *callLog
}

func (f *fakeSnapshotApplier) InstallSnapshot(ctx context.Context, snap Snapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.installed = append(f.installed, cloneSnapshot(snap))
	if f.calls != nil {
		f.calls.add(fmt.Sprintf("snapshot.install:%d", snap.EndOffset))
	}
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

type testEnv struct {
	t           testing.TB
	localNode   NodeID
	log         *fakeLogStore
	checkpoints *fakeCheckpointStore
	history     *fakeEpochHistoryStore
	snapshots   *fakeSnapshotApplier
	clock       *manualClock
	calls       *callLog
	replica     *replica
}

func newTestEnv(t testing.TB) *testEnv {
	t.Helper()

	calls := &callLog{}
	return &testEnv{
		t:           t,
		localNode:   1,
		log:         &fakeLogStore{calls: calls, appendSignal: make(chan uint64, 8)},
		checkpoints: &fakeCheckpointStore{loadErr: ErrEmptyState, calls: calls},
		history:     &fakeEpochHistoryStore{loadErr: ErrEmptyState, calls: calls},
		snapshots:   &fakeSnapshotApplier{calls: calls},
		clock:       newManualClock(time.Unix(1_700_000_000, 0).UTC()),
		calls:       calls,
	}
}

func (e *testEnv) config() ReplicaConfig {
	return ReplicaConfig{
		LocalNode:         e.localNode,
		LogStore:          e.log,
		CheckpointStore:   e.checkpoints,
		EpochHistoryStore: e.history,
		SnapshotApplier:   e.snapshots,
		Now:               e.clock.Now,
	}
}

func newReplicaFromEnv(t testing.TB, env *testEnv) *replica {
	t.Helper()

	got, err := NewReplica(env.config())
	if err != nil {
		t.Fatalf("NewReplica() error = %v", err)
	}

	r, ok := got.(*replica)
	if !ok {
		t.Fatalf("NewReplica() type = %T", got)
	}
	env.replica = r
	return r
}

func newTestReplica(t testing.TB) *replica {
	t.Helper()
	return newReplicaFromEnv(t, newTestEnv(t))
}

func cloneRecord(record Record) Record {
	record.Payload = append([]byte(nil), record.Payload...)
	return record
}

func cloneSnapshot(snap Snapshot) Snapshot {
	snap.Payload = append([]byte(nil), snap.Payload...)
	return snap
}

type callLog struct {
	mu    sync.Mutex
	calls []string
}

func (c *callLog) add(call string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, call)
}

func (c *callLog) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

func newRecoveredLeaderEnv(t testing.TB) *testEnv {
	t.Helper()

	env := newTestEnv(t)
	env.log.leo = 4
	env.checkpoints.loadErr = nil
	env.checkpoints.checkpoint = Checkpoint{
		Epoch:          6,
		LogStartOffset: 0,
		HW:             4,
	}
	env.history.loadErr = nil
	env.history.points = []EpochPoint{{Epoch: 6, StartOffset: 0}}
	env.replica = newReplicaFromEnv(t, env)
	return env
}

func newFetchEnvWithHistory(t testing.TB) *testEnv {
	t.Helper()

	env := newTestEnv(t)
	env.log.records = []Record{
		{Payload: []byte("r0"), SizeBytes: 1},
		{Payload: []byte("r1"), SizeBytes: 1},
		{Payload: []byte("r2"), SizeBytes: 1},
		{Payload: []byte("r3"), SizeBytes: 1},
	}
	env.log.leo = 4
	env.checkpoints.loadErr = nil
	env.checkpoints.checkpoint = Checkpoint{
		Epoch:          3,
		LogStartOffset: 0,
		HW:             4,
	}
	env.history.loadErr = nil
	env.history.points = []EpochPoint{{Epoch: 3, StartOffset: 0}}
	env.replica = newReplicaFromEnv(t, env)
	env.replica.mustApplyMeta(t, activeMeta(7, 1))
	if err := env.replica.BecomeLeader(activeMeta(7, 1)); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}
	env.log.records = append(env.log.records,
		Record{Payload: []byte("r4"), SizeBytes: 1},
		Record{Payload: []byte("r5"), SizeBytes: 1},
	)
	env.log.leo = 6
	env.replica.state.LEO = 6
	return env
}

func newLeaderReplica(t testing.TB) *replica {
	t.Helper()
	return newFetchEnvWithHistory(t).replica
}

func newFollowerEnv(t testing.TB) *testEnv {
	t.Helper()

	env := newTestEnv(t)
	env.localNode = 2
	env.checkpoints.loadErr = nil
	env.checkpoints.checkpoint = Checkpoint{
		Epoch:          7,
		LogStartOffset: 0,
		HW:             0,
	}
	env.history.loadErr = nil
	env.history.points = []EpochPoint{{Epoch: 7, StartOffset: 0}}
	env.replica = newReplicaFromEnv(t, env)
	env.replica.mustApplyMeta(t, activeMeta(7, 1))
	if err := env.replica.BecomeFollower(activeMeta(7, 1)); err != nil {
		t.Fatalf("BecomeFollower() error = %v", err)
	}
	return env
}

func activeMeta(epoch uint64, leader NodeID) GroupMeta {
	return activeMetaWithMinISR(epoch, leader, 2)
}

func activeMetaWithMinISR(epoch uint64, leader NodeID, minISR int) GroupMeta {
	return GroupMeta{
		GroupID:    10,
		Epoch:      epoch,
		Leader:     leader,
		Replicas:   []NodeID{1, 2, 3},
		ISR:        []NodeID{1, 2, 3},
		MinISR:     minISR,
		LeaseUntil: time.Unix(1_700_000_300, 0).UTC(),
	}
}

func (r *replica) mustApplyMeta(t testing.TB, meta GroupMeta) {
	t.Helper()
	if err := r.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
}

func newFollowerReplica(t testing.TB) *replica {
	t.Helper()
	return newFollowerEnv(t).replica
}

type threeReplicaCluster struct {
	leader    *replica
	follower2 *replica
	follower3 *replica
}

func newThreeReplicaCluster(t testing.TB) *threeReplicaCluster {
	t.Helper()

	meta := activeMetaWithMinISR(7, 1, 3)

	leaderEnv := newTestEnv(t)
	leaderEnv.replica = newReplicaFromEnv(t, leaderEnv)
	leaderEnv.replica.mustApplyMeta(t, meta)
	if err := leaderEnv.replica.BecomeLeader(meta); err != nil {
		t.Fatalf("leader BecomeLeader() error = %v", err)
	}

	follower2Env := newTestEnv(t)
	follower2Env.localNode = 2
	follower2Env.replica = newReplicaFromEnv(t, follower2Env)
	follower2Env.replica.mustApplyMeta(t, meta)
	if err := follower2Env.replica.BecomeFollower(meta); err != nil {
		t.Fatalf("follower2 BecomeFollower() error = %v", err)
	}

	follower3Env := newTestEnv(t)
	follower3Env.localNode = 3
	follower3Env.replica = newReplicaFromEnv(t, follower3Env)
	follower3Env.replica.mustApplyMeta(t, meta)
	if err := follower3Env.replica.BecomeFollower(meta); err != nil {
		t.Fatalf("follower3 BecomeFollower() error = %v", err)
	}

	return &threeReplicaCluster{
		leader:    leaderEnv.replica,
		follower2: follower2Env.replica,
		follower3: follower3Env.replica,
	}
}

func (c *threeReplicaCluster) replicateOnce(t testing.TB, follower *replica) {
	t.Helper()

	req := FetchRequest{
		GroupID:     c.leader.state.GroupID,
		Epoch:       c.leader.state.Epoch,
		ReplicaID:   follower.localNode,
		FetchOffset: follower.state.LEO,
		OffsetEpoch: follower.state.Epoch,
		MaxBytes:    1024,
	}
	result, err := c.leader.Fetch(context.Background(), req)
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if err := follower.ApplyFetch(context.Background(), ApplyFetchRequest{
		GroupID:    req.GroupID,
		Epoch:      result.Epoch,
		Leader:     c.leader.localNode,
		TruncateTo: result.TruncateTo,
		Records:    result.Records,
		LeaderHW:   result.HW,
	}); err != nil {
		t.Fatalf("ApplyFetch() error = %v", err)
	}
	if follower.state.LEO == 0 {
		t.Fatal("follower LEO did not advance after ApplyFetch")
	}
	_, err = c.leader.Fetch(context.Background(), FetchRequest{
		GroupID:     req.GroupID,
		Epoch:       result.Epoch,
		ReplicaID:   follower.localNode,
		FetchOffset: follower.state.LEO,
		OffsetEpoch: follower.state.Epoch,
		MaxBytes:    1024,
	})
	if err != nil {
		t.Fatalf("ack Fetch() error = %v", err)
	}
	if c.leader.progress[follower.localNode] != follower.state.LEO {
		t.Fatalf("leader progress[%d] = %d, follower LEO = %d", follower.localNode, c.leader.progress[follower.localNode], follower.state.LEO)
	}
}

func waitForLogAppend(t testing.TB, log *fakeLogStore, want uint64) {
	t.Helper()

	deadline := time.After(time.Second)
	for {
		select {
		case got := <-log.appendSignal:
			if got == want {
				return
			}
		case <-deadline:
			log.mu.Lock()
			got := log.leo
			log.mu.Unlock()
			t.Fatalf("log LEO = %d, want %d", got, want)
			return
		}
	}
}
