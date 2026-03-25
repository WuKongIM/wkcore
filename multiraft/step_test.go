package multiraft

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

func TestStepRoutesMessageToCorrectGroup(t *testing.T) {
	rt := newStartedRuntime(t)
	if err := rt.OpenGroup(context.Background(), newInternalGroupOptions(100)); err != nil {
		t.Fatalf("OpenGroup() error = %v", err)
	}
	if err := rt.Step(context.Background(), Envelope{
		GroupID: 100,
		Message: raftpb.Message{Type: raftpb.MsgHeartbeat, From: 2, To: 1},
	}); err != nil {
		t.Fatalf("Step() error = %v", err)
	}

	waitForCondition(t, func() bool { return groupRequestCount(rt, 100) == 1 })
}

func TestStepUnknownGroupReturnsErrGroupNotFound(t *testing.T) {
	rt := newStartedRuntime(t)
	err := rt.Step(context.Background(), Envelope{GroupID: 404})
	if !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("expected ErrGroupNotFound, got %v", err)
	}
}

func TestRuntimeTickLoopEnqueuesOpenGroups(t *testing.T) {
	rt := newStartedRuntime(t)
	if err := rt.OpenGroup(context.Background(), newInternalGroupOptions(101)); err != nil {
		t.Fatalf("OpenGroup() error = %v", err)
	}

	waitForCondition(t, func() bool { return groupTickCount(rt, 101) > 0 })
}

func TestWrapMessagesClonesRaftMessagePayloads(t *testing.T) {
	msgs := []raftpb.Message{{
		Type:    raftpb.MsgApp,
		From:    1,
		To:      2,
		Context: []byte("ctx"),
		Entries: []raftpb.Entry{{
			Index: 1,
			Term:  1,
			Data:  []byte("entry"),
		}},
		Snapshot: &raftpb.Snapshot{
			Data: []byte("snap"),
			Metadata: raftpb.SnapshotMetadata{
				Index: 1,
				Term:  1,
				ConfState: raftpb.ConfState{
					Voters: []uint64{1, 2, 3},
				},
			},
		},
	}}

	batch := wrapMessages(42, msgs)

	msgs[0].Context[0] = 'x'
	msgs[0].Entries[0].Data[0] = 'X'
	msgs[0].Snapshot.Data[0] = 'Y'
	msgs[0].Snapshot.Metadata.ConfState.Voters[0] = 99

	got := batch[0].Message
	if string(got.Context) != "ctx" {
		t.Fatalf("Context = %q", got.Context)
	}
	if string(got.Entries[0].Data) != "entry" {
		t.Fatalf("Entries[0].Data = %q", got.Entries[0].Data)
	}
	if got.Snapshot == nil || string(got.Snapshot.Data) != "snap" {
		t.Fatalf("Snapshot.Data = %v", got.Snapshot)
	}
	if got.Snapshot.Metadata.ConfState.Voters[0] != 1 {
		t.Fatalf("Snapshot.Metadata.ConfState.Voters[0] = %d", got.Snapshot.Metadata.ConfState.Voters[0])
	}
}

func newStartedRuntime(t *testing.T) *Runtime {
	t.Helper()

	rt, err := New(Options{
		NodeID:       1,
		TickInterval: 10 * time.Millisecond,
		Workers:      1,
		Transport:    &internalFakeTransport{},
		Raft: RaftOptions{
			ElectionTick:  10,
			HeartbeatTick: 1,
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return rt
}

func newInternalGroupOptions(id GroupID) GroupOptions {
	return GroupOptions{
		ID:           id,
		Storage:      &internalFakeStorage{},
		StateMachine: &internalFakeStateMachine{},
	}
}

func waitForCondition(t *testing.T, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not satisfied before timeout")
}

func groupRequestCount(rt *Runtime, id GroupID) int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	g := rt.groups[id]
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.requestCount
}

func groupTickCount(rt *Runtime, id GroupID) int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	g := rt.groups[id]
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.tickCount
}

type internalFakeTransport struct{}

func (f *internalFakeTransport) Send(ctx context.Context, batch []Envelope) error {
	return nil
}

type internalFakeStorage struct {
	mu             sync.Mutex
	state          BootstrapState
	entries        []raftpb.Entry
	snapshot       raftpb.Snapshot
	saveCount      int
	lastSavedIndex uint64
	lastApplied    uint64
}

func (f *internalFakeStorage) InitialState(ctx context.Context) (BootstrapState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state, nil
}

func (f *internalFakeStorage) Entries(ctx context.Context, lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []raftpb.Entry
	for _, entry := range f.entries {
		if entry.Index >= lo && entry.Index < hi {
			out = append(out, entry)
		}
	}
	return out, nil
}

func (f *internalFakeStorage) Term(ctx context.Context, index uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, entry := range f.entries {
		if entry.Index == index {
			return entry.Term, nil
		}
	}
	return 0, nil
}

func (f *internalFakeStorage) FirstIndex(ctx context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.entries) == 0 {
		return 1, nil
	}
	return f.entries[0].Index, nil
}

func (f *internalFakeStorage) LastIndex(ctx context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.entries) == 0 {
		return f.snapshot.Metadata.Index, nil
	}
	return f.entries[len(f.entries)-1].Index, nil
}

func (f *internalFakeStorage) Snapshot(ctx context.Context) (raftpb.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapshot, nil
}

func (f *internalFakeStorage) Save(ctx context.Context, st PersistentState) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.saveCount++
	if st.HardState != nil {
		f.state.HardState = *st.HardState
		if st.HardState.Commit > f.lastSavedIndex {
			f.lastSavedIndex = st.HardState.Commit
		}
	}
	if len(st.Entries) > 0 {
		f.entries = append([]raftpb.Entry(nil), st.Entries...)
		f.lastSavedIndex = st.Entries[len(st.Entries)-1].Index
	}
	if st.Snapshot != nil {
		f.snapshot = *st.Snapshot
		f.lastSavedIndex = st.Snapshot.Metadata.Index
	}
	return nil
}

func (f *internalFakeStorage) MarkApplied(ctx context.Context, index uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.lastApplied = index
	f.state.AppliedIndex = index
	return nil
}

type internalFakeStateMachine struct {
	mu           sync.Mutex
	applied      [][]byte
	restoreCount int
	lastSnapshot Snapshot
}

func (f *internalFakeStateMachine) Apply(ctx context.Context, cmd Command) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.applied = append(f.applied, append([]byte(nil), cmd.Data...))
	return append([]byte("ok:"), cmd.Data...), nil
}

func (f *internalFakeStateMachine) Restore(ctx context.Context, snap Snapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restoreCount++
	f.lastSnapshot = snap
	return nil
}

func (f *internalFakeStateMachine) Snapshot(ctx context.Context) (Snapshot, error) {
	return Snapshot{}, nil
}
