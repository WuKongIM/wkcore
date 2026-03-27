package wkdbraft

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/wraft/multiraft"
	"github.com/WuKongIM/wraft/wkdb"
	"go.etcd.io/raft/v3/raftpb"
)

func TestWKDBBackedGroupRestoresStateFromSnapshotOnReopen(t *testing.T) {
	ctx := context.Background()
	groupID := multiraft.GroupID(51)

	db := openTestDB(t)

	store := NewStorage(db, uint64(groupID))
	fsm := NewStateMachine(db, uint64(groupID))

	rt := newStartedRuntime(t)
	if err := rt.BootstrapGroup(ctx, multiraft.BootstrapGroupRequest{
		Group: multiraft.GroupOptions{
			ID:           groupID,
			Storage:      store,
			StateMachine: fsm,
		},
		Voters: []multiraft.NodeID{1},
	}); err != nil {
		t.Fatalf("BootstrapGroup() error = %v", err)
	}

	waitForCondition(t, func() bool {
		st, err := rt.Status(groupID)
		return err == nil && st.Role == multiraft.RoleLeader
	})

	fut, err := rt.Propose(ctx, groupID, mustJSON(t, map[string]any{
		"type": "upsert_user",
		"user": wkdb.User{
			UID:         "u1",
			Token:       "t1",
			DeviceFlag:  1,
			DeviceLevel: 2,
		},
	}))
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}

	result, err := fut.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	innerFSM := wkdb.NewStateMachine(db, uint64(groupID))
	snap, err := innerFSM.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if len(snap.Data) == 0 {
		t.Fatal("Snapshot().Data is empty")
	}

	innerStore := wkdb.NewRaftStorage(db, uint64(groupID))
	if err := innerStore.Save(ctx, wkdb.RaftPersistentState{
		Snapshot: &raftpb.Snapshot{
			Data: snap.Data,
			Metadata: raftpb.SnapshotMetadata{
				Index: result.Index,
				Term:  result.Term,
				ConfState: raftpb.ConfState{
					Voters: []uint64{1},
				},
			},
		},
	}); err != nil {
		t.Fatalf("Save(snapshot) error = %v", err)
	}

	if err := rt.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := db.DeleteSlotData(ctx, uint64(groupID)); err != nil {
		t.Fatalf("DeleteSlotData() error = %v", err)
	}
	if _, err := db.ForSlot(uint64(groupID)).GetUser(ctx, "u1"); !errors.Is(err, wkdb.ErrNotFound) {
		t.Fatalf("GetUser() after delete err = %v, want ErrNotFound", err)
	}

	reopenRT := newStartedRuntime(t)
	if err := reopenRT.OpenGroup(ctx, multiraft.GroupOptions{
		ID:           groupID,
		Storage:      NewStorage(db, uint64(groupID)),
		StateMachine: NewStateMachine(db, uint64(groupID)),
	}); err != nil {
		t.Fatalf("OpenGroup() error = %v", err)
	}

	gotUser, err := db.ForSlot(uint64(groupID)).GetUser(ctx, "u1")
	if err != nil {
		t.Fatalf("GetUser() after reopen: %v", err)
	}
	if gotUser.Token != "t1" {
		t.Fatalf("restored user = %#v", gotUser)
	}
}

func newStartedRuntime(t *testing.T) *multiraft.Runtime {
	t.Helper()

	rt, err := multiraft.New(multiraft.Options{
		NodeID:       1,
		TickInterval: 10 * time.Millisecond,
		Workers:      1,
		Transport:    fakeTransport{},
		Raft: multiraft.RaftOptions{
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

type fakeTransport struct{}

func (fakeTransport) Send(ctx context.Context, batch []multiraft.Envelope) error {
	return nil
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

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()

	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return data
}
