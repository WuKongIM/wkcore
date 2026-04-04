package metafsm

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
)

func TestStateMachineEncodeUpsertCommands(t *testing.T) {
	userCmd := EncodeUpsertUserCommand(metadb.User{UID: "u1", Token: "t1", DeviceFlag: 3, DeviceLevel: 7})
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

	channelCmd := EncodeUpsertChannelCommand(metadb.Channel{ChannelID: "c1", ChannelType: 1, Ban: 1})
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

	metaCmd := EncodeUpsertChannelRuntimeMetaCommand(metadb.ChannelRuntimeMeta{
		ChannelID:    "c1",
		ChannelType:  1,
		ChannelEpoch: 3,
		LeaderEpoch:  2,
		Replicas:     []uint64{3, 1, 2},
		ISR:          []uint64{2, 1},
		Leader:       1,
		MinISR:       2,
		Status:       3,
		Features:     9,
		LeaseUntilMS: 1700000000000,
	})
	decoded, err = decodeCommand(metaCmd)
	if err != nil {
		t.Fatalf("decodeCommand(runtime_meta) error = %v", err)
	}
	mc, ok := decoded.(*upsertChannelRuntimeMetaCmd)
	if !ok {
		t.Fatalf("decodeCommand(runtime_meta) type = %T, want *upsertChannelRuntimeMetaCmd", decoded)
	}
	wantMeta := metadb.ChannelRuntimeMeta{
		ChannelID:    "c1",
		ChannelType:  1,
		ChannelEpoch: 3,
		LeaderEpoch:  2,
		Replicas:     []uint64{1, 2, 3},
		ISR:          []uint64{1, 2},
		Leader:       1,
		MinISR:       2,
		Status:       3,
		Features:     9,
		LeaseUntilMS: 1700000000000,
	}
	if !reflect.DeepEqual(mc.meta, wantMeta) {
		t.Fatalf("decoded runtime meta = %#v, want %#v", mc.meta, wantMeta)
	}

	createUserData := EncodeCreateUserCommand(metadb.User{UID: "u-create", Token: "create-token", DeviceFlag: 4, DeviceLevel: 8})
	decoded, err = decodeCommand(createUserData)
	if err != nil {
		t.Fatalf("decodeCommand(create_user) error = %v", err)
	}
	cuc, ok := decoded.(*createUserCmd)
	if !ok {
		t.Fatalf("decodeCommand(create_user) type = %T, want *createUserCmd", decoded)
	}
	if cuc.user.UID != "u-create" || cuc.user.Token != "create-token" || cuc.user.DeviceFlag != 4 || cuc.user.DeviceLevel != 8 {
		t.Fatalf("decoded create user = %+v", cuc.user)
	}

	deviceData := EncodeUpsertDeviceCommand(metadb.Device{UID: "u-device", DeviceFlag: 6, Token: "device-token", DeviceLevel: 9})
	decoded, err = decodeCommand(deviceData)
	if err != nil {
		t.Fatalf("decodeCommand(device) error = %v", err)
	}
	dc, ok := decoded.(*upsertDeviceCmd)
	if !ok {
		t.Fatalf("decodeCommand(device) type = %T, want *upsertDeviceCmd", decoded)
	}
	if dc.device.UID != "u-device" || dc.device.DeviceFlag != 6 || dc.device.Token != "device-token" || dc.device.DeviceLevel != 9 {
		t.Fatalf("decoded device = %+v", dc.device)
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
		Data:    EncodeUpsertUserCommand(metadb.User{UID: "u1", Token: "t1", DeviceFlag: 1, DeviceLevel: 2}),
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
		Data:    EncodeUpsertUserCommand(metadb.User{UID: "u1", Token: "t2", DeviceFlag: 5, DeviceLevel: 9}),
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
		Data:    EncodeUpsertChannelCommand(metadb.Channel{ChannelID: "c1", ChannelType: 1, Ban: 1}),
	}); err != nil {
		t.Fatalf("Apply(channel create) error = %v", err)
	}

	if _, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   4,
		Term:    1,
		Data:    EncodeUpsertChannelCommand(metadb.Channel{ChannelID: "c1", ChannelType: 1, Ban: 9}),
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

func TestStateMachineApplyUpsertsAndDeletesChannelRuntimeMeta(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)

	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    "c-meta",
		ChannelType:  4,
		ChannelEpoch: 7,
		LeaderEpoch:  5,
		Replicas:     []uint64{3, 2, 1},
		ISR:          []uint64{2, 1},
		Leader:       2,
		MinISR:       2,
		Status:       4,
		Features:     12,
		LeaseUntilMS: 1700000001234,
	}

	result, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   1,
		Term:    1,
		Data:    EncodeUpsertChannelRuntimeMetaCommand(meta),
	})
	if err != nil {
		t.Fatalf("Apply(upsert runtime meta) error = %v", err)
	}
	if string(result) != ApplyResultOK {
		t.Fatalf("Apply(upsert runtime meta) result = %q, want %q", result, ApplyResultOK)
	}

	got, err := db.ForSlot(11).GetChannelRuntimeMeta(ctx, meta.ChannelID, meta.ChannelType)
	if err != nil {
		t.Fatalf("GetChannelRuntimeMeta() error = %v", err)
	}
	want := meta
	want.Replicas = []uint64{1, 2, 3}
	want.ISR = []uint64{1, 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stored runtime meta = %#v, want %#v", got, want)
	}

	if _, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   2,
		Term:    1,
		Data:    EncodeDeleteChannelRuntimeMetaCommand(meta.ChannelID, meta.ChannelType),
	}); err != nil {
		t.Fatalf("Apply(delete runtime meta) error = %v", err)
	}

	_, err = db.ForSlot(11).GetChannelRuntimeMeta(ctx, meta.ChannelID, meta.ChannelType)
	if !errors.Is(err, metadb.ErrNotFound) {
		t.Fatalf("GetChannelRuntimeMeta() err = %v, want ErrNotFound", err)
	}
}

func TestStateMachineApplyCreateUserAndUpsertDevice(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)

	result, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   1,
		Term:    1,
		Data:    EncodeCreateUserCommand(metadb.User{UID: "u1", Token: "create-token", DeviceFlag: 1, DeviceLevel: 2}),
	})
	if err != nil {
		t.Fatalf("Apply(create user) error = %v", err)
	}
	if string(result) != ApplyResultOK {
		t.Fatalf("Apply(create user) result = %q, want %q", result, ApplyResultOK)
	}

	gotUser, err := db.ForSlot(11).GetUser(ctx, "u1")
	if err != nil {
		t.Fatalf("GetUser() error = %v", err)
	}
	if gotUser.Token != "create-token" || gotUser.DeviceFlag != 1 || gotUser.DeviceLevel != 2 {
		t.Fatalf("created user = %#v", gotUser)
	}

	if _, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   2,
		Term:    1,
		Data:    EncodeCreateUserCommand(metadb.User{UID: "u1", Token: "overwrite-attempt", DeviceFlag: 9, DeviceLevel: 9}),
	}); err != nil {
		t.Fatalf("Apply(duplicate create user) error = %v", err)
	}

	gotUser, err = db.ForSlot(11).GetUser(ctx, "u1")
	if err != nil {
		t.Fatalf("GetUser() after duplicate create error = %v", err)
	}
	if gotUser.Token != "create-token" || gotUser.DeviceFlag != 1 || gotUser.DeviceLevel != 2 {
		t.Fatalf("user after duplicate create = %#v", gotUser)
	}

	if _, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   3,
		Term:    1,
		Data:    EncodeUpsertDeviceCommand(metadb.Device{UID: "u1", DeviceFlag: 1, Token: "app-token", DeviceLevel: 5}),
	}); err != nil {
		t.Fatalf("Apply(upsert device) error = %v", err)
	}

	gotDevice, err := db.ForSlot(11).GetDevice(ctx, "u1", 1)
	if err != nil {
		t.Fatalf("GetDevice() error = %v", err)
	}
	if gotDevice.Token != "app-token" || gotDevice.DeviceLevel != 5 {
		t.Fatalf("stored device = %#v", gotDevice)
	}
}

func TestStateMachineRejectsIncompleteChannelRuntimeMetaCommand(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)

	cmd := make([]byte, 0, headerSize+32)
	cmd = append(cmd, commandVersion, cmdTypeUpsertChannelRuntimeMeta)
	cmd = appendStringTLVField(cmd, tagRuntimeMetaChannelID, "partial-meta")
	cmd = appendInt64TLVField(cmd, tagRuntimeMetaChannelType, 1)

	_, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   1,
		Term:    1,
		Data:    cmd,
	})
	if !errors.Is(err, metadb.ErrCorruptValue) {
		t.Fatalf("Apply(incomplete runtime meta) err = %v, want ErrCorruptValue", err)
	}
}

func TestStateMachineRejectsIncompleteDeleteChannelRuntimeMetaCommand(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)

	cmd := make([]byte, 0, headerSize+32)
	cmd = append(cmd, commandVersion, cmdTypeDeleteChannelRuntimeMeta)
	cmd = appendStringTLVField(cmd, tagRuntimeMetaChannelID, "partial-delete")

	_, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   1,
		Term:    1,
		Data:    cmd,
	})
	if !errors.Is(err, metadb.ErrCorruptValue) {
		t.Fatalf("Apply(incomplete delete runtime meta) err = %v, want ErrCorruptValue", err)
	}
}

func TestStateMachineRejectsDeleteChannelRuntimeMetaWithEmptyChannelID(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)

	_, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   1,
		Term:    1,
		Data:    EncodeDeleteChannelRuntimeMetaCommand("", 1),
	})
	if !errors.Is(err, metadb.ErrInvalidArgument) {
		t.Fatalf("Apply(delete runtime meta with empty channel id) err = %v, want ErrInvalidArgument", err)
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
		Data:    EncodeUpsertUserCommand(metadb.User{UID: "u1", Token: "t1", DeviceFlag: 1, DeviceLevel: 2}),
	}); err != nil {
		t.Fatalf("Apply(user) error = %v", err)
	}
	if _, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   2,
		Term:    1,
		Data:    EncodeUpsertChannelCommand(metadb.Channel{ChannelID: "c1", ChannelType: 1, Ban: 1}),
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
		Data:    EncodeUpsertUserCommand(metadb.User{UID: "u1", Token: "slot11"}),
	}); err != nil {
		t.Fatalf("Apply(slot11) error = %v", err)
	}
	if err := db.ForSlot(12).CreateUser(ctx, metadb.User{UID: "u1", Token: "slot12"}); err != nil {
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
	if !errors.Is(err, metadb.ErrNotFound) {
		t.Fatalf("GetUser(slot12) err = %v, want ErrNotFound", err)
	}
}

func TestStateMachineSnapshotRestoreIncludesChannelRuntimeMeta(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	sm := mustNewStateMachine(t, db, 11)

	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    "snap-meta",
		ChannelType:  8,
		ChannelEpoch: 4,
		LeaderEpoch:  2,
		Replicas:     []uint64{3, 1, 2},
		ISR:          []uint64{2, 1},
		Leader:       1,
		MinISR:       2,
		Status:       3,
		Features:     15,
		LeaseUntilMS: 1700000005678,
	}

	if _, err := sm.Apply(ctx, multiraft.Command{
		GroupID: 11,
		Index:   1,
		Term:    1,
		Data:    EncodeUpsertChannelRuntimeMetaCommand(meta),
	}); err != nil {
		t.Fatalf("Apply(runtime meta) error = %v", err)
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

	got, err := restoreDB.ForSlot(11).GetChannelRuntimeMeta(ctx, meta.ChannelID, meta.ChannelType)
	if err != nil {
		t.Fatalf("GetChannelRuntimeMeta() error = %v", err)
	}

	want := meta
	want.Replicas = []uint64{1, 2, 3}
	want.ISR = []uint64{1, 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restored runtime meta = %#v, want %#v", got, want)
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
		Data:    EncodeUpsertUserCommand(metadb.User{UID: "u1", Token: "t1"}),
	})
	if !errors.Is(err, metadb.ErrInvalidArgument) {
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
	if !errors.Is(err, metadb.ErrCorruptValue) {
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
	if !errors.Is(err, metadb.ErrInvalidArgument) {
		t.Fatalf("Apply(unknown) err = %v, want ErrInvalidArgument", err)
	}
}

func TestNewStateMachineValidation(t *testing.T) {
	db := openTestDB(t)

	if _, err := NewStateMachine(nil, 1); !errors.Is(err, metadb.ErrInvalidArgument) {
		t.Fatalf("NewStateMachine(nil, 1) err = %v, want ErrInvalidArgument", err)
	}
	if _, err := NewStateMachine(db, 0); !errors.Is(err, metadb.ErrInvalidArgument) {
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
		{GroupID: 11, Index: 1, Term: 1, Data: EncodeUpsertUserCommand(metadb.User{UID: "u1", Token: "t1", DeviceFlag: 1, DeviceLevel: 2})},
		{GroupID: 11, Index: 2, Term: 1, Data: EncodeUpsertChannelCommand(metadb.Channel{ChannelID: "c1", ChannelType: 1, Ban: 3})},
		{GroupID: 11, Index: 3, Term: 1, Data: EncodeUpsertUserCommand(metadb.User{UID: "u2", Token: "t2", DeviceFlag: 4, DeviceLevel: 5})},
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
		{GroupID: 11, Index: 1, Term: 1, Data: EncodeUpsertUserCommand(metadb.User{UID: "u1", Token: "t1"})},
		{GroupID: 99, Index: 2, Term: 1, Data: EncodeUpsertUserCommand(metadb.User{UID: "u2", Token: "t2"})},
	}

	_, err := bsm.ApplyBatch(ctx, cmds)
	if !errors.Is(err, metadb.ErrInvalidArgument) {
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
		{GroupID: 11, Index: 1, Term: 1, Data: EncodeUpsertUserCommand(metadb.User{UID: "u-atomic", Token: "t1"})},
		{GroupID: 11, Index: 2, Term: 1, Data: []byte{commandVersion, 0xFF}}, // unknown type
	}

	_, err := bsm.ApplyBatch(ctx, cmds)
	if err == nil {
		t.Fatal("ApplyBatch() expected error for invalid command")
	}

	// Verify no partial writes: u-atomic should not exist.
	_, err = db.ForSlot(11).GetUser(ctx, "u-atomic")
	if !errors.Is(err, metadb.ErrNotFound) {
		t.Fatalf("GetUser(u-atomic) err = %v, want ErrNotFound (no partial writes)", err)
	}
}

func TestApplyBatchCreateUserPreservesFirstWriteWithinBatch(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	bsm, ok := mustNewStateMachine(t, db, 11).(multiraft.BatchStateMachine)
	if !ok {
		t.Fatal("state machine does not implement multiraft.BatchStateMachine")
	}

	cmds := []multiraft.Command{
		{GroupID: 11, Index: 1, Term: 1, Data: EncodeCreateUserCommand(metadb.User{UID: "u-atomic", Token: "first"})},
		{GroupID: 11, Index: 2, Term: 1, Data: EncodeCreateUserCommand(metadb.User{UID: "u-atomic", Token: "second"})},
	}

	results, err := bsm.ApplyBatch(ctx, cmds)
	if err != nil {
		t.Fatalf("ApplyBatch() error = %v", err)
	}
	if len(results) != 2 || string(results[0]) != ApplyResultOK || string(results[1]) != ApplyResultOK {
		t.Fatalf("ApplyBatch() results = %q", results)
	}

	got, err := db.ForSlot(11).GetUser(ctx, "u-atomic")
	if err != nil {
		t.Fatalf("GetUser() error = %v", err)
	}
	if got.Token != "first" {
		t.Fatalf("stored user = %#v", got)
	}
}

func TestApplyBatchCreateUserDoesNotOverwritePriorUpsertWithinBatch(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	bsm, ok := mustNewStateMachine(t, db, 11).(multiraft.BatchStateMachine)
	if !ok {
		t.Fatal("state machine does not implement multiraft.BatchStateMachine")
	}

	cmds := []multiraft.Command{
		{GroupID: 11, Index: 1, Term: 1, Data: EncodeUpsertUserCommand(metadb.User{UID: "u-upsert", Token: "upserted", DeviceFlag: 7, DeviceLevel: 8})},
		{GroupID: 11, Index: 2, Term: 1, Data: EncodeCreateUserCommand(metadb.User{UID: "u-upsert", Token: "created", DeviceFlag: 1, DeviceLevel: 2})},
	}

	_, err := bsm.ApplyBatch(ctx, cmds)
	if err != nil {
		t.Fatalf("ApplyBatch() error = %v", err)
	}

	got, err := db.ForSlot(11).GetUser(ctx, "u-upsert")
	if err != nil {
		t.Fatalf("GetUser() error = %v", err)
	}
	if got.Token != "upserted" || got.DeviceFlag != 7 || got.DeviceLevel != 8 {
		t.Fatalf("stored user = %#v", got)
	}
}

// --- Encode/decode edge case tests ---

func TestEncodeDecodeEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		user metadb.User
	}{
		{
			name: "empty UID and Token",
			user: metadb.User{UID: "", Token: "", DeviceFlag: 0, DeviceLevel: 0},
		},
		{
			name: "zero-value fields",
			user: metadb.User{UID: "u", Token: "t", DeviceFlag: 0, DeviceLevel: 0},
		},
		{
			name: "MaxInt64 fields",
			user: metadb.User{UID: "u", Token: "t", DeviceFlag: math.MaxInt64, DeviceLevel: math.MaxInt64},
		},
		{
			name: "negative int64 fields",
			user: metadb.User{UID: "u", Token: "t", DeviceFlag: math.MinInt64, DeviceLevel: -1},
		},
		{
			name: "long strings (1KB+)",
			user: metadb.User{UID: strings.Repeat("x", 1024), Token: strings.Repeat("y", 2048), DeviceFlag: 1, DeviceLevel: 2},
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
	createCmd := EncodeUpsertChannelCommand(metadb.Channel{
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
	if err != metadb.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

func TestEncodeDecodeChannelEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		channel metadb.Channel
	}{
		{
			name:    "empty ChannelID",
			channel: metadb.Channel{ChannelID: "", ChannelType: 0, Ban: 0},
		},
		{
			name:    "MaxInt64 fields",
			channel: metadb.Channel{ChannelID: "c1", ChannelType: math.MaxInt64, Ban: math.MaxInt64},
		},
		{
			name:    "long ChannelID (1KB+)",
			channel: metadb.Channel{ChannelID: strings.Repeat("z", 1024), ChannelType: 1, Ban: 0},
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
