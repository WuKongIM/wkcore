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
	t           *testing.T
	localNode   NodeID
	log         *fakeLogStore
	checkpoints *fakeCheckpointStore
	history     *fakeEpochHistoryStore
	snapshots   *fakeSnapshotApplier
	clock       *manualClock
	calls       *callLog
	replica     *replica
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	calls := &callLog{}
	return &testEnv{
		t:           t,
		localNode:   1,
		log:         &fakeLogStore{calls: calls},
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

func newReplicaFromEnv(t *testing.T, env *testEnv) *replica {
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

func newTestReplica(t *testing.T) *replica {
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

func newRecoveredLeaderEnv(t *testing.T) *testEnv {
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

func newFetchEnvWithHistory(t *testing.T) *testEnv {
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

func newLeaderReplica(t *testing.T) *replica {
	t.Helper()
	return newFetchEnvWithHistory(t).replica
}

func newFollowerEnv(t *testing.T) *testEnv {
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
	return GroupMeta{
		GroupID:    10,
		Epoch:      epoch,
		Leader:     leader,
		Replicas:   []NodeID{1, 2, 3},
		ISR:        []NodeID{1, 2, 3},
		MinISR:     2,
		LeaseUntil: time.Unix(1_700_000_300, 0).UTC(),
	}
}

func (r *replica) mustApplyMeta(t *testing.T, meta GroupMeta) {
	t.Helper()
	if err := r.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
}
