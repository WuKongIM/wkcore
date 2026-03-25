package multiraft

import (
	"context"
	"errors"
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

type internalFakeStorage struct{}

func (f *internalFakeStorage) InitialState(ctx context.Context) (BootstrapState, error) {
	return BootstrapState{}, nil
}

func (f *internalFakeStorage) Entries(ctx context.Context, lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	return nil, nil
}

func (f *internalFakeStorage) Term(ctx context.Context, index uint64) (uint64, error) {
	return 0, nil
}

func (f *internalFakeStorage) FirstIndex(ctx context.Context) (uint64, error) {
	return 0, nil
}

func (f *internalFakeStorage) LastIndex(ctx context.Context) (uint64, error) {
	return 0, nil
}

func (f *internalFakeStorage) Snapshot(ctx context.Context) (raftpb.Snapshot, error) {
	return raftpb.Snapshot{}, nil
}

func (f *internalFakeStorage) Save(ctx context.Context, st PersistentState) error {
	return nil
}

func (f *internalFakeStorage) MarkApplied(ctx context.Context, index uint64) error {
	return nil
}

type internalFakeStateMachine struct{}

func (f *internalFakeStateMachine) Apply(ctx context.Context, cmd Command) ([]byte, error) {
	return nil, nil
}

func (f *internalFakeStateMachine) Restore(ctx context.Context, snap Snapshot) error {
	return nil
}

func (f *internalFakeStateMachine) Snapshot(ctx context.Context) (Snapshot, error) {
	return Snapshot{}, nil
}
