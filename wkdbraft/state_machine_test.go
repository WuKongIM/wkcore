package wkdbraft

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/WuKongIM/wraft/wkdb"
)

func openTestDB(t *testing.T) *wkdb.DB {
	t.Helper()
	db, err := wkdb.Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return db
}

func TestWKDBStateMachineSnapshotRestoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := wkdb.NewStateMachine(db, 11)

	if _, err := sm.Apply(ctx, wkdb.RaftCommand{
		GroupID: 11,
		Index:   1,
		Term:    1,
		Data:    wkdb.EncodeUpsertUserCommand(wkdb.User{UID: "u1", Token: "t1", DeviceFlag: 1, DeviceLevel: 2}),
	}); err != nil {
		t.Fatalf("Apply(user): %v", err)
	}
	if _, err := sm.Apply(ctx, wkdb.RaftCommand{
		GroupID: 11,
		Index:   2,
		Term:    1,
		Data:    wkdb.EncodeUpsertChannelCommand(wkdb.Channel{ChannelID: "c1", ChannelType: 1, Ban: 1}),
	}); err != nil {
		t.Fatalf("Apply(channel): %v", err)
	}

	snap, err := sm.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	if len(snap.Data) == 0 {
		t.Fatal("Snapshot() returned empty Data")
	}

	restoreDB := openTestDB(t)
	restoreSM := wkdb.NewStateMachine(restoreDB, 11)
	if err := restoreSM.Restore(ctx, snap); err != nil {
		t.Fatalf("Restore(): %v", err)
	}

	restoreShard := restoreDB.ForSlot(11)
	gotUser, err := restoreShard.GetUser(ctx, "u1")
	if err != nil {
		t.Fatalf("GetUser(): %v", err)
	}
	if gotUser.Token != "t1" {
		t.Fatalf("restored user = %#v", gotUser)
	}

	gotChannel, err := restoreShard.GetChannel(ctx, "c1", 1)
	if err != nil {
		t.Fatalf("GetChannel(): %v", err)
	}
	if gotChannel.Ban != 1 {
		t.Fatalf("restored channel = %#v", gotChannel)
	}
}

func TestWKDBStateMachineSnapshotIsSlotScoped(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := wkdb.NewStateMachine(db, 11)

	if _, err := sm.Apply(ctx, wkdb.RaftCommand{
		GroupID: 11,
		Index:   1,
		Term:    1,
		Data:    wkdb.EncodeUpsertUserCommand(wkdb.User{UID: "u1", Token: "slot11"}),
	}); err != nil {
		t.Fatalf("Apply(slot11): %v", err)
	}
	if err := db.ForSlot(12).CreateUser(ctx, wkdb.User{UID: "u1", Token: "slot12"}); err != nil {
		t.Fatalf("CreateUser(slot12): %v", err)
	}

	snap, err := sm.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}

	restoreDB := openTestDB(t)
	restoreSM := wkdb.NewStateMachine(restoreDB, 11)
	if err := restoreSM.Restore(ctx, snap); err != nil {
		t.Fatalf("Restore(): %v", err)
	}

	restoreSlot11 := restoreDB.ForSlot(11)
	gotUser, err := restoreSlot11.GetUser(ctx, "u1")
	if err != nil {
		t.Fatalf("GetUser(slot11): %v", err)
	}
	if gotUser.Token != "slot11" {
		t.Fatalf("slot11 user = %#v", gotUser)
	}

	_, err = restoreDB.ForSlot(12).GetUser(ctx, "u1")
	if !errors.Is(err, wkdb.ErrNotFound) {
		t.Fatalf("GetUser(slot12) err = %v, want ErrNotFound", err)
	}
}
