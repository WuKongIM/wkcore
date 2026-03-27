package wkfsm

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/WuKongIM/wraft/multiraft"
	"github.com/WuKongIM/wraft/wkdb"
)

func TestStateMachineEncodeUpsertCommands(t *testing.T) {
	userCmd := EncodeUpsertUserCommand(wkdb.User{UID: "u1", Token: "t1"})
	channelCmd := EncodeUpsertChannelCommand(wkdb.Channel{ChannelID: "c1", ChannelType: 1, Ban: 1})

	var userDecoded map[string]any
	if err := json.Unmarshal(userCmd, &userDecoded); err != nil {
		t.Fatalf("json.Unmarshal(user) error = %v", err)
	}
	if userDecoded["type"] != "upsert_user" {
		t.Fatalf("user type = %#v, want %q", userDecoded["type"], "upsert_user")
	}
	if _, ok := userDecoded["user"]; !ok {
		t.Fatal("user payload missing")
	}

	var channelDecoded map[string]any
	if err := json.Unmarshal(channelCmd, &channelDecoded); err != nil {
		t.Fatalf("json.Unmarshal(channel) error = %v", err)
	}
	if channelDecoded["type"] != "upsert_channel" {
		t.Fatalf("channel type = %#v, want %q", channelDecoded["type"], "upsert_channel")
	}
	if _, ok := channelDecoded["channel"]; !ok {
		t.Fatal("channel payload missing")
	}
}

func TestStateMachineApplyUpsertsUserAndChannel(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := New(db, 11)

	result, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   1,
		Term:    1,
		Data:    EncodeUpsertUserCommand(wkdb.User{UID: "u1", Token: "t1", DeviceFlag: 1, DeviceLevel: 2}),
	})
	if err != nil {
		t.Fatalf("Apply(user create) error = %v", err)
	}
	if string(result) != "ok" {
		t.Fatalf("Apply(user create) result = %q, want %q", result, "ok")
	}

	if _, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   2,
		Term:    1,
		Data:    EncodeUpsertUserCommand(wkdb.User{UID: "u1", Token: "t2", DeviceFlag: 5, DeviceLevel: 9}),
	}); err != nil {
		t.Fatalf("Apply(user update) error = %v", err)
	}

	gotUser, err := db.ForSlot(11).GetUser(ctx, "u1")
	if err != nil {
		t.Fatalf("GetUser() error = %v", err)
	}
	if gotUser.Token != "t2" || gotUser.DeviceFlag != 5 || gotUser.DeviceLevel != 9 {
		t.Fatalf("updated user = %#v", gotUser)
	}

	if _, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   3,
		Term:    1,
		Data:    EncodeUpsertChannelCommand(wkdb.Channel{ChannelID: "c1", ChannelType: 1, Ban: 1}),
	}); err != nil {
		t.Fatalf("Apply(channel create) error = %v", err)
	}

	if _, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   4,
		Term:    1,
		Data:    EncodeUpsertChannelCommand(wkdb.Channel{ChannelID: "c1", ChannelType: 1, Ban: 9}),
	}); err != nil {
		t.Fatalf("Apply(channel update) error = %v", err)
	}

	gotChannel, err := db.ForSlot(11).GetChannel(ctx, "c1", 1)
	if err != nil {
		t.Fatalf("GetChannel() error = %v", err)
	}
	if gotChannel.Ban != 9 {
		t.Fatalf("updated channel = %#v", gotChannel)
	}
}

func TestStateMachineSnapshotRestoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := New(db, 11)

	if _, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   1,
		Term:    1,
		Data:    EncodeUpsertUserCommand(wkdb.User{UID: "u1", Token: "t1", DeviceFlag: 1, DeviceLevel: 2}),
	}); err != nil {
		t.Fatalf("Apply(user) error = %v", err)
	}
	if _, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   2,
		Term:    1,
		Data:    EncodeUpsertChannelCommand(wkdb.Channel{ChannelID: "c1", ChannelType: 1, Ban: 1}),
	}); err != nil {
		t.Fatalf("Apply(channel) error = %v", err)
	}

	snap, err := sm.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if len(snap.Data) == 0 {
		t.Fatal("Snapshot().Data is empty")
	}

	restoreDB := openTestDB(t)
	restoreSM := New(restoreDB, 11)
	if err := restoreSM.Restore(ctx, snap); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}

	gotUser, err := restoreDB.ForSlot(11).GetUser(ctx, "u1")
	if err != nil {
		t.Fatalf("GetUser() error = %v", err)
	}
	if gotUser.Token != "t1" {
		t.Fatalf("restored user = %#v", gotUser)
	}

	gotChannel, err := restoreDB.ForSlot(11).GetChannel(ctx, "c1", 1)
	if err != nil {
		t.Fatalf("GetChannel() error = %v", err)
	}
	if gotChannel.Ban != 1 {
		t.Fatalf("restored channel = %#v", gotChannel)
	}
}

func TestStateMachineSnapshotIsSlotScoped(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := New(db, 11)

	if _, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   1,
		Term:    1,
		Data:    EncodeUpsertUserCommand(wkdb.User{UID: "u1", Token: "slot11"}),
	}); err != nil {
		t.Fatalf("Apply(slot11) error = %v", err)
	}
	if err := db.ForSlot(12).CreateUser(ctx, wkdb.User{UID: "u1", Token: "slot12"}); err != nil {
		t.Fatalf("CreateUser(slot12) error = %v", err)
	}

	snap, err := sm.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	restoreDB := openTestDB(t)
	restoreSM := New(restoreDB, 11)
	if err := restoreSM.Restore(ctx, snap); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}

	gotUser, err := restoreDB.ForSlot(11).GetUser(ctx, "u1")
	if err != nil {
		t.Fatalf("GetUser(slot11) error = %v", err)
	}
	if gotUser.Token != "slot11" {
		t.Fatalf("slot11 user = %#v", gotUser)
	}

	_, err = restoreDB.ForSlot(12).GetUser(ctx, "u1")
	if !errors.Is(err, wkdb.ErrNotFound) {
		t.Fatalf("GetUser(slot12) err = %v, want ErrNotFound", err)
	}
}

func TestStateMachineRejectsMismatchedGroupID(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := New(db, 11)

	_, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 12,
		Index:   1,
		Term:    1,
		Data:    EncodeUpsertUserCommand(wkdb.User{UID: "u1", Token: "t1"}),
	})
	if !errors.Is(err, wkdb.ErrInvalidArgument) {
		t.Fatalf("Apply() err = %v, want ErrInvalidArgument", err)
	}
}

func TestStateMachineRejectsMalformedJSON(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := New(db, 11)

	_, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   1,
		Term:    1,
		Data:    []byte("{"),
	})
	if !errors.Is(err, wkdb.ErrCorruptValue) {
		t.Fatalf("Apply(malformed) err = %v, want ErrCorruptValue", err)
	}
}

func TestStateMachineRejectsUnknownCommand(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := New(db, 11)

	_, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   1,
		Term:    1,
		Data:    []byte(`{"type":"unknown"}`),
	})
	if !errors.Is(err, wkdb.ErrInvalidArgument) {
		t.Fatalf("Apply(unknown) err = %v, want ErrInvalidArgument", err)
	}
}
