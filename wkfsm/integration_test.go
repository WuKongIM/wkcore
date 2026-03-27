package wkfsm

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/wraft/multiraft"
	"github.com/WuKongIM/wraft/raftstore"
	"github.com/WuKongIM/wraft/wkdb"
)

func TestMemoryBackedGroupAppliesProposalToWKDB(t *testing.T) {
	ctx := context.Background()
	groupID := multiraft.GroupID(51)
	db := openTestDB(t)
	rt := newStartedRuntime(t)

	if err := rt.BootstrapGroup(ctx, multiraft.BootstrapGroupRequest{
		Group: multiraft.GroupOptions{
			ID:           groupID,
			Storage:      raftstore.NewMemory(),
			StateMachine: New(db, uint64(groupID)),
		},
		Voters: []multiraft.NodeID{1},
	}); err != nil {
		t.Fatalf("BootstrapGroup() error = %v", err)
	}

	waitForCondition(t, func() bool {
		st, err := rt.Status(groupID)
		return err == nil && st.Role == multiraft.RoleLeader
	})

	fut, err := rt.Propose(ctx, groupID, EncodeUpsertUserCommand(wkdb.User{
		UID:         "u1",
		Token:       "t1",
		DeviceFlag:  1,
		DeviceLevel: 2,
	}))
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}

	if _, err := fut.Wait(ctx); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	got, err := db.ForSlot(uint64(groupID)).GetUser(ctx, "u1")
	if err != nil {
		t.Fatalf("GetUser() error = %v", err)
	}
	if got.Token != "t1" {
		t.Fatalf("stored user = %#v", got)
	}
}

func TestMemoryBackedGroupDoesNotRecoverDeletedSlotDataAfterOpenGroup(t *testing.T) {
	ctx := context.Background()
	groupID := multiraft.GroupID(51)
	db := openTestDB(t)

	rt := newStartedRuntime(t)
	if err := rt.BootstrapGroup(ctx, multiraft.BootstrapGroupRequest{
		Group: multiraft.GroupOptions{
			ID:           groupID,
			Storage:      raftstore.NewMemory(),
			StateMachine: New(db, uint64(groupID)),
		},
		Voters: []multiraft.NodeID{1},
	}); err != nil {
		t.Fatalf("BootstrapGroup() error = %v", err)
	}

	waitForCondition(t, func() bool {
		st, err := rt.Status(groupID)
		return err == nil && st.Role == multiraft.RoleLeader
	})

	fut, err := rt.Propose(ctx, groupID, EncodeUpsertUserCommand(wkdb.User{
		UID:   "u1",
		Token: "t1",
	}))
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	if _, err := fut.Wait(ctx); err != nil {
		t.Fatalf("Wait() error = %v", err)
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
		Storage:      raftstore.NewMemory(),
		StateMachine: New(db, uint64(groupID)),
	}); err != nil {
		t.Fatalf("OpenGroup() error = %v", err)
	}

	_, err = db.ForSlot(uint64(groupID)).GetUser(ctx, "u1")
	if !errors.Is(err, wkdb.ErrNotFound) {
		t.Fatalf("GetUser() after reopen err = %v, want ErrNotFound", err)
	}
}
