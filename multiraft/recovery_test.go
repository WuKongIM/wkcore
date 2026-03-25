package multiraft

import (
	"context"
	"testing"

	"go.etcd.io/raft/v3/raftpb"
)

func TestOpenGroupRestoresAppliedIndexFromStorage(t *testing.T) {
	store := &internalFakeStorage{
		state: BootstrapState{
			HardState: raftpb.HardState{
				Commit: 7,
			},
			AppliedIndex: 7,
		},
		snapshot: raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: 7,
				Term:  1,
			},
		},
	}
	rt := newStartedRuntime(t)
	if err := rt.OpenGroup(context.Background(), GroupOptions{
		ID:           40,
		Storage:      store,
		StateMachine: &internalFakeStateMachine{},
	}); err != nil {
		t.Fatalf("OpenGroup() error = %v", err)
	}
	st, err := rt.Status(40)
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	if st.AppliedIndex != 7 {
		t.Fatalf("Status().AppliedIndex = %d", st.AppliedIndex)
	}
}

func TestOpenGroupRestoresSnapshotIntoStateMachine(t *testing.T) {
	fsm := &internalFakeStateMachine{}
	store := &internalFakeStorage{
		state: BootstrapState{
			HardState: raftpb.HardState{
				Commit: 5,
			},
			AppliedIndex: 5,
		},
		snapshot: raftpb.Snapshot{
			Data: []byte("snap"),
			Metadata: raftpb.SnapshotMetadata{
				Index: 5,
				Term:  2,
			},
		},
	}
	rt := newStartedRuntime(t)
	if err := rt.OpenGroup(context.Background(), GroupOptions{
		ID:           41,
		Storage:      store,
		StateMachine: fsm,
	}); err != nil {
		t.Fatalf("OpenGroup() error = %v", err)
	}

	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	if fsm.restoreCount != 1 {
		t.Fatalf("Restore() count = %d", fsm.restoreCount)
	}
	if fsm.lastSnapshot.Index != 5 {
		t.Fatalf("Restore() snapshot index = %d", fsm.lastSnapshot.Index)
	}
}
