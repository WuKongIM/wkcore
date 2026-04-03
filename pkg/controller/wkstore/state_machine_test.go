package wkstore

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/controller/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
)

func TestStateMachineEncodeUpsertCommands(t *testing.T) {
	userCmd := EncodeUpsertUserCommand(wkdb.User{UID: "u1", Token: "t1", DeviceFlag: 3, DeviceLevel: 7})
	decoded, err := decodeCommand(userCmd)
	if err != nil {
		t.Fatalf("decodeCommand(user) error = %v", err)
	}
	uc, ok := decoded.(*upsertUserCmd)
	if !ok {
		t.Fatalf("decodeCommand(user) type = %T, want *upsertUserCmd", decoded)
	}
	if uc.user.UID != "u1" || uc.user.Token != "t1" || uc.user.DeviceFlag != 3 || uc.user.DeviceLevel != 7 {
		t.Fatalf("decoded user = %+v", uc.user)
	}

	channelCmd := EncodeUpsertChannelCommand(wkdb.Channel{ChannelID: "c1", ChannelType: 1, Ban: 1})
	decoded, err = decodeCommand(channelCmd)
	if err != nil {
		t.Fatalf("decodeCommand(channel) error = %v", err)
	}
	cc, ok := decoded.(*upsertChannelCmd)
	if !ok {
		t.Fatalf("decodeCommand(channel) type = %T, want *upsertChannelCmd", decoded)
	}
	if cc.channel.ChannelID != "c1" || cc.channel.ChannelType != 1 || cc.channel.Ban != 1 {
		t.Fatalf("decoded channel = %+v", cc.channel)
	}
}

func TestStateMachineApplyUpsertsUserAndChannel(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)

	result, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   1,
		Term:    1,
		Data:    EncodeUpsertUserCommand(wkdb.User{UID: "u1", Token: "t1", DeviceFlag: 1, DeviceLevel: 2}),
	})
	if err != nil {
		t.Fatalf("Apply(user create) error = %v", err)
	}
	if string(result) != ApplyResultOK {
		t.Fatalf("Apply(user create) result = %q, want %q", result, ApplyResultOK)
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
	sm := mustNewStateMachine(t, db, 11)

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
	restoreSM := mustNewStateMachine(t, restoreDB, 11)
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
	sm := mustNewStateMachine(t, db, 11)

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
	restoreSM := mustNewStateMachine(t, restoreDB, 11)
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
	sm := mustNewStateMachine(t, db, 11)

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

func TestStateMachineRejectsTruncatedCommand(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)

	_, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   1,
		Term:    1,
		Data:    []byte{0x01}, // only version byte, missing cmdType
	})
	if !errors.Is(err, wkdb.ErrCorruptValue) {
		t.Fatalf("Apply(truncated) err = %v, want ErrCorruptValue", err)
	}
}

func TestStateMachineRejectsUnknownCommand(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)

	_, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   1,
		Term:    1,
		Data:    []byte{commandVersion, 0xFF}, // valid header, unknown command type
	})
	if !errors.Is(err, wkdb.ErrInvalidArgument) {
		t.Fatalf("Apply(unknown) err = %v, want ErrInvalidArgument", err)
	}
}

func TestNewStateMachineValidation(t *testing.T) {
	db := openTestDB(t)

	if _, err := NewStateMachine(nil, 1); !errors.Is(err, wkdb.ErrInvalidArgument) {
		t.Fatalf("NewStateMachine(nil, 1) err = %v, want ErrInvalidArgument", err)
	}
	if _, err := NewStateMachine(db, 0); !errors.Is(err, wkdb.ErrInvalidArgument) {
		t.Fatalf("NewStateMachine(db, 0) err = %v, want ErrInvalidArgument", err)
	}
	sm, err := NewStateMachine(db, 1)
	if err != nil {
		t.Fatalf("NewStateMachine(db, 1) error = %v", err)
	}
	if sm == nil {
		t.Fatal("NewStateMachine returned nil")
	}
}

// --- ApplyBatch tests ---

func TestApplyBatchUpsertsMultipleCommands(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)
	bsm := sm.(multiraft.BatchStateMachine)

	cmds := []multiraft.Command{
		{GroupID: 11, Index: 1, Term: 1, Data: EncodeUpsertUserCommand(wkdb.User{UID: "u1", Token: "t1", DeviceFlag: 1, DeviceLevel: 2})},
		{GroupID: 11, Index: 2, Term: 1, Data: EncodeUpsertChannelCommand(wkdb.Channel{ChannelID: "c1", ChannelType: 1, Ban: 3})},
		{GroupID: 11, Index: 3, Term: 1, Data: EncodeUpsertUserCommand(wkdb.User{UID: "u2", Token: "t2", DeviceFlag: 4, DeviceLevel: 5})},
	}

	results, err := bsm.ApplyBatch(ctx, cmds)
	if err != nil {
		t.Fatalf("ApplyBatch() error = %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("ApplyBatch() returned %d results, want 3", len(results))
	}
	for i, r := range results {
		if string(r) != ApplyResultOK {
			t.Fatalf("result[%d] = %q, want %q", i, r, ApplyResultOK)
		}
	}

	shard := db.ForSlot(11)
	gotU1, err := shard.GetUser(ctx, "u1")
	if err != nil {
		t.Fatalf("GetUser(u1) error = %v", err)
	}
	if gotU1.Token != "t1" || gotU1.DeviceFlag != 1 || gotU1.DeviceLevel != 2 {
		t.Fatalf("u1 = %+v", gotU1)
	}

	gotU2, err := shard.GetUser(ctx, "u2")
	if err != nil {
		t.Fatalf("GetUser(u2) error = %v", err)
	}
	if gotU2.Token != "t2" || gotU2.DeviceFlag != 4 || gotU2.DeviceLevel != 5 {
		t.Fatalf("u2 = %+v", gotU2)
	}

	gotCh, err := shard.GetChannel(ctx, "c1", 1)
	if err != nil {
		t.Fatalf("GetChannel(c1) error = %v", err)
	}
	if gotCh.Ban != 3 {
		t.Fatalf("c1 = %+v", gotCh)
	}
}

func TestApplyBatchRejectsOnMismatchedGroupID(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)
	bsm := sm.(multiraft.BatchStateMachine)

	cmds := []multiraft.Command{
		{GroupID: 11, Index: 1, Term: 1, Data: EncodeUpsertUserCommand(wkdb.User{UID: "u1", Token: "t1"})},
		{GroupID: 99, Index: 2, Term: 1, Data: EncodeUpsertUserCommand(wkdb.User{UID: "u2", Token: "t2"})},
	}

	_, err := bsm.ApplyBatch(ctx, cmds)
	if !errors.Is(err, wkdb.ErrInvalidArgument) {
		t.Fatalf("ApplyBatch() err = %v, want ErrInvalidArgument", err)
	}
}

func TestApplyBatchAtomicity(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)
	bsm := sm.(multiraft.BatchStateMachine)

	// First command is valid, second has invalid data — the whole batch should fail.
	cmds := []multiraft.Command{
		{GroupID: 11, Index: 1, Term: 1, Data: EncodeUpsertUserCommand(wkdb.User{UID: "u-atomic", Token: "t1"})},
		{GroupID: 11, Index: 2, Term: 1, Data: []byte{commandVersion, 0xFF}}, // unknown type
	}

	_, err := bsm.ApplyBatch(ctx, cmds)
	if err == nil {
		t.Fatal("ApplyBatch() expected error for invalid command")
	}

	// Verify no partial writes: u-atomic should not exist.
	_, err = db.ForSlot(11).GetUser(ctx, "u-atomic")
	if !errors.Is(err, wkdb.ErrNotFound) {
		t.Fatalf("GetUser(u-atomic) err = %v, want ErrNotFound (no partial writes)", err)
	}
}

// --- Encode/decode edge case tests ---

func TestEncodeDecodeEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		user wkdb.User
	}{
		{
			name: "empty UID and Token",
			user: wkdb.User{UID: "", Token: "", DeviceFlag: 0, DeviceLevel: 0},
		},
		{
			name: "zero-value fields",
			user: wkdb.User{UID: "u", Token: "t", DeviceFlag: 0, DeviceLevel: 0},
		},
		{
			name: "MaxInt64 fields",
			user: wkdb.User{UID: "u", Token: "t", DeviceFlag: math.MaxInt64, DeviceLevel: math.MaxInt64},
		},
		{
			name: "negative int64 fields",
			user: wkdb.User{UID: "u", Token: "t", DeviceFlag: math.MinInt64, DeviceLevel: -1},
		},
		{
			name: "long strings (1KB+)",
			user: wkdb.User{UID: strings.Repeat("x", 1024), Token: strings.Repeat("y", 2048), DeviceFlag: 1, DeviceLevel: 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeUpsertUserCommand(tt.user)
			decoded, err := decodeCommand(encoded)
			if err != nil {
				t.Fatalf("decodeCommand() error = %v", err)
			}
			uc, ok := decoded.(*upsertUserCmd)
			if !ok {
				t.Fatalf("type = %T, want *upsertUserCmd", decoded)
			}
			if uc.user.UID != tt.user.UID {
				t.Fatalf("UID = %q, want %q", uc.user.UID, tt.user.UID)
			}
			if uc.user.Token != tt.user.Token {
				t.Fatalf("Token = %q, want %q", uc.user.Token, tt.user.Token)
			}
			if uc.user.DeviceFlag != tt.user.DeviceFlag {
				t.Fatalf("DeviceFlag = %d, want %d", uc.user.DeviceFlag, tt.user.DeviceFlag)
			}
			if uc.user.DeviceLevel != tt.user.DeviceLevel {
				t.Fatalf("DeviceLevel = %d, want %d", uc.user.DeviceLevel, tt.user.DeviceLevel)
			}
		})
	}
}

func TestApplyBatch_DeleteChannel(t *testing.T) {
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 1)
	ctx := context.Background()

	// First create a channel via upsert
	createCmd := EncodeUpsertChannelCommand(wkdb.Channel{
		ChannelID: "ch1", ChannelType: 1, Ban: 0,
	})
	_, err := sm.Apply(ctx, multiraft.Command{GroupID: 1, Data: createCmd})
	if err != nil {
		t.Fatalf("Apply upsert: %v", err)
	}

	// Verify channel exists
	ch, err := db.ForSlot(1).GetChannel(ctx, "ch1", 1)
	if err != nil {
		t.Fatalf("GetChannel after create: %v", err)
	}
	if ch.ChannelID != "ch1" {
		t.Fatalf("unexpected channel ID: %s", ch.ChannelID)
	}

	// Delete the channel
	deleteCmd := EncodeDeleteChannelCommand("ch1", 1)
	_, err = sm.Apply(ctx, multiraft.Command{GroupID: 1, Data: deleteCmd})
	if err != nil {
		t.Fatalf("Apply delete: %v", err)
	}

	// Verify channel is gone
	_, err = db.ForSlot(1).GetChannel(ctx, "ch1", 1)
	if err != wkdb.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

func TestEncodeDecodeChannelEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		channel wkdb.Channel
	}{
		{
			name:    "empty ChannelID",
			channel: wkdb.Channel{ChannelID: "", ChannelType: 0, Ban: 0},
		},
		{
			name:    "MaxInt64 fields",
			channel: wkdb.Channel{ChannelID: "c1", ChannelType: math.MaxInt64, Ban: math.MaxInt64},
		},
		{
			name:    "long ChannelID (1KB+)",
			channel: wkdb.Channel{ChannelID: strings.Repeat("z", 1024), ChannelType: 1, Ban: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeUpsertChannelCommand(tt.channel)
			decoded, err := decodeCommand(encoded)
			if err != nil {
				t.Fatalf("decodeCommand() error = %v", err)
			}
			cc, ok := decoded.(*upsertChannelCmd)
			if !ok {
				t.Fatalf("type = %T, want *upsertChannelCmd", decoded)
			}
			if cc.channel.ChannelID != tt.channel.ChannelID {
				t.Fatalf("ChannelID = %q, want %q", cc.channel.ChannelID, tt.channel.ChannelID)
			}
			if cc.channel.ChannelType != tt.channel.ChannelType {
				t.Fatalf("ChannelType = %d, want %d", cc.channel.ChannelType, tt.channel.ChannelType)
			}
			if cc.channel.Ban != tt.channel.Ban {
				t.Fatalf("Ban = %d, want %d", cc.channel.Ban, tt.channel.Ban)
			}
		})
	}
}
