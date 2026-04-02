package isr

import (
	"context"
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
	return nil
}

type fakeCheckpointStore struct {
	mu         sync.Mutex
	checkpoint Checkpoint
	loadErr    error
	stored     []Checkpoint
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
}

func (f *fakeEpochHistoryStore) Load() ([]EpochPoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]EpochPoint(nil), f.points...), f.loadErr
}

func (f *fakeEpochHistoryStore) Append(point EpochPoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.points = append(f.points, point)
	f.appended = append(f.appended, point)
	return nil
}

type fakeSnapshotApplier struct {
	mu        sync.Mutex
	installed []Snapshot
}

func (f *fakeSnapshotApplier) InstallSnapshot(ctx context.Context, snap Snapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.installed = append(f.installed, cloneSnapshot(snap))
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
	log         *fakeLogStore
	checkpoints *fakeCheckpointStore
	history     *fakeEpochHistoryStore
	snapshots   *fakeSnapshotApplier
	clock       *manualClock
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	return &testEnv{
		t:           t,
		log:         &fakeLogStore{},
		checkpoints: &fakeCheckpointStore{loadErr: ErrEmptyState},
		history:     &fakeEpochHistoryStore{loadErr: ErrEmptyState},
		snapshots:   &fakeSnapshotApplier{},
		clock:       newManualClock(time.Unix(1_700_000_000, 0).UTC()),
	}
}

func (e *testEnv) config() ReplicaConfig {
	return ReplicaConfig{
		LocalNode:         1,
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
