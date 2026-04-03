package wkstore

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/controller/raftstore"
	"github.com/WuKongIM/WuKongIM/pkg/controller/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
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
			StateMachine: mustNewStateMachine(t, db, uint64(groupID)),
		},
		Voters: []multiraft.NodeID{1},
	}); err != nil {
		t.Fatalf("BootstrapGroup() error = %v", err)
	}

	waitForCondition(t, func() bool {
		st, err := rt.Status(groupID)
		return err == nil && st.Role == multiraft.RoleLeader
	}, "group become leader")

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
			StateMachine: mustNewStateMachine(t, db, uint64(groupID)),
		},
		Voters: []multiraft.NodeID{1},
	}); err != nil {
		t.Fatalf("BootstrapGroup() error = %v", err)
	}

	waitForCondition(t, func() bool {
		st, err := rt.Status(groupID)
		return err == nil && st.Role == multiraft.RoleLeader
	}, "group become leader")

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
		StateMachine: mustNewStateMachine(t, db, uint64(groupID)),
	}); err != nil {
		t.Fatalf("OpenGroup() error = %v", err)
	}

	_, err = db.ForSlot(uint64(groupID)).GetUser(ctx, "u1")
	if !errors.Is(err, wkdb.ErrNotFound) {
		t.Fatalf("GetUser() after reopen err = %v, want ErrNotFound", err)
	}
}

func TestMemoryBackedGroupReopensWithRecoveredMembership(t *testing.T) {
	ctx := context.Background()
	groupID := multiraft.GroupID(52)
	db := openTestDB(t)
	store := raftstore.NewMemory()

	rt := newStartedRuntime(t)
	if err := rt.BootstrapGroup(ctx, multiraft.BootstrapGroupRequest{
		Group: multiraft.GroupOptions{
			ID:           groupID,
			Storage:      store,
			StateMachine: mustNewStateMachine(t, db, uint64(groupID)),
		},
		Voters: []multiraft.NodeID{1},
	}); err != nil {
		t.Fatalf("BootstrapGroup() error = %v", err)
	}

	waitForCondition(t, func() bool {
		st, err := rt.Status(groupID)
		return err == nil && st.Role == multiraft.RoleLeader
	}, "group become leader")

	fut, err := rt.Propose(ctx, groupID, EncodeUpsertUserCommand(wkdb.User{
		UID:   "u1",
		Token: "before-reopen",
	}))
	if err != nil {
		t.Fatalf("Propose(before reopen) error = %v", err)
	}
	if _, err := fut.Wait(ctx); err != nil {
		t.Fatalf("Wait(before reopen) error = %v", err)
	}

	if err := rt.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopenRT := newStartedRuntime(t)
	if err := reopenRT.OpenGroup(ctx, multiraft.GroupOptions{
		ID:           groupID,
		Storage:      store,
		StateMachine: mustNewStateMachine(t, db, uint64(groupID)),
	}); err != nil {
		t.Fatalf("OpenGroup() error = %v", err)
	}

	waitForCondition(t, func() bool {
		st, err := reopenRT.Status(groupID)
		return err == nil && st.Role == multiraft.RoleLeader
	}, "reopened group become leader")

	fut, err = reopenRT.Propose(ctx, groupID, EncodeUpsertUserCommand(wkdb.User{
		UID:   "u1",
		Token: "after-reopen",
	}))
	if err != nil {
		t.Fatalf("Propose(after reopen) error = %v", err)
	}
	if _, err := fut.Wait(ctx); err != nil {
		t.Fatalf("Wait(after reopen) error = %v", err)
	}

	got, err := db.ForSlot(uint64(groupID)).GetUser(ctx, "u1")
	if err != nil {
		t.Fatalf("GetUser() error = %v", err)
	}
	if got.Token != "after-reopen" {
		t.Fatalf("stored user = %#v", got)
	}
}

func TestPebbleBackedGroupReopensAndAcceptsNewProposal(t *testing.T) {
	ctx := context.Background()
	groupID := multiraft.GroupID(61)
	root := t.TempDir()
	bizPath := filepath.Join(root, "biz")
	raftPath := filepath.Join(root, "raft")

	bizDB := openTestDBAt(t, bizPath)
	raftDB := openTestRaftDBAt(t, raftPath)

	rt := newStartedRuntime(t)
	if err := rt.BootstrapGroup(ctx, multiraft.BootstrapGroupRequest{
		Group: multiraft.GroupOptions{
			ID:           groupID,
			Storage:      raftDB.ForGroup(uint64(groupID)),
			StateMachine: mustNewStateMachine(t, bizDB, uint64(groupID)),
		},
		Voters: []multiraft.NodeID{1},
	}); err != nil {
		t.Fatalf("BootstrapGroup() error = %v", err)
	}

	waitForCondition(t, func() bool {
		st, err := rt.Status(groupID)
		return err == nil && st.Role == multiraft.RoleLeader
	}, "group become leader")

	fut, err := rt.Propose(ctx, groupID, EncodeUpsertUserCommand(wkdb.User{
		UID:   "u1",
		Token: "before-reopen",
	}))
	if err != nil {
		t.Fatalf("Propose(before reopen) error = %v", err)
	}
	if _, err := fut.Wait(ctx); err != nil {
		t.Fatalf("Wait(before reopen) error = %v", err)
	}

	if err := rt.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := bizDB.Close(); err != nil {
		t.Fatalf("bizDB.Close() error = %v", err)
	}
	if err := raftDB.Close(); err != nil {
		t.Fatalf("raftDB.Close() error = %v", err)
	}

	reopenedBizDB := openTestDBAt(t, bizPath)
	reopenedRaftDB := openTestRaftDBAt(t, raftPath)
	reopenRT := newStartedRuntime(t)
	if err := reopenRT.OpenGroup(ctx, multiraft.GroupOptions{
		ID:           groupID,
		Storage:      reopenedRaftDB.ForGroup(uint64(groupID)),
		StateMachine: mustNewStateMachine(t, reopenedBizDB, uint64(groupID)),
	}); err != nil {
		t.Fatalf("OpenGroup() error = %v", err)
	}

	waitForCondition(t, func() bool {
		st, err := reopenRT.Status(groupID)
		return err == nil && st.Role == multiraft.RoleLeader
	}, "reopened group become leader")

	fut, err = reopenRT.Propose(ctx, groupID, EncodeUpsertUserCommand(wkdb.User{
		UID:   "u1",
		Token: "after-reopen",
	}))
	if err != nil {
		t.Fatalf("Propose(after reopen) error = %v", err)
	}
	if _, err := fut.Wait(ctx); err != nil {
		t.Fatalf("Wait(after reopen) error = %v", err)
	}

	got, err := reopenedBizDB.ForSlot(uint64(groupID)).GetUser(ctx, "u1")
	if err != nil {
		t.Fatalf("GetUser() error = %v", err)
	}
	if got.Token != "after-reopen" {
		t.Fatalf("stored user = %#v", got)
	}
}

func TestPebbleBackedGroupDoesNotRecoverDeletedBusinessStateWithoutSnapshot(t *testing.T) {
	ctx := context.Background()
	groupID := multiraft.GroupID(62)
	root := t.TempDir()
	bizPath := filepath.Join(root, "biz")
	raftPath := filepath.Join(root, "raft")

	bizDB := openTestDBAt(t, bizPath)
	raftDB := openTestRaftDBAt(t, raftPath)

	rt := newStartedRuntime(t)
	if err := rt.BootstrapGroup(ctx, multiraft.BootstrapGroupRequest{
		Group: multiraft.GroupOptions{
			ID:           groupID,
			Storage:      raftDB.ForGroup(uint64(groupID)),
			StateMachine: mustNewStateMachine(t, bizDB, uint64(groupID)),
		},
		Voters: []multiraft.NodeID{1},
	}); err != nil {
		t.Fatalf("BootstrapGroup() error = %v", err)
	}

	waitForCondition(t, func() bool {
		st, err := rt.Status(groupID)
		return err == nil && st.Role == multiraft.RoleLeader
	}, "group become leader")

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
	if err := bizDB.Close(); err != nil {
		t.Fatalf("bizDB.Close() error = %v", err)
	}
	if err := raftDB.Close(); err != nil {
		t.Fatalf("raftDB.Close() error = %v", err)
	}

	reopenedBizDB := openTestDBAt(t, bizPath)
	reopenedRaftDB := openTestRaftDBAt(t, raftPath)

	if err := reopenedBizDB.DeleteSlotData(ctx, uint64(groupID)); err != nil {
		t.Fatalf("DeleteSlotData() error = %v", err)
	}
	if _, err := reopenedBizDB.ForSlot(uint64(groupID)).GetUser(ctx, "u1"); !errors.Is(err, wkdb.ErrNotFound) {
		t.Fatalf("GetUser() after delete err = %v, want ErrNotFound", err)
	}

	reopenRT := newStartedRuntime(t)
	if err := reopenRT.OpenGroup(ctx, multiraft.GroupOptions{
		ID:           groupID,
		Storage:      reopenedRaftDB.ForGroup(uint64(groupID)),
		StateMachine: mustNewStateMachine(t, reopenedBizDB, uint64(groupID)),
	}); err != nil {
		t.Fatalf("OpenGroup() error = %v", err)
	}

	if _, err := reopenedBizDB.ForSlot(uint64(groupID)).GetUser(ctx, "u1"); !errors.Is(err, wkdb.ErrNotFound) {
		t.Fatalf("GetUser() after reopen err = %v, want ErrNotFound", err)
	}
}
