package metastore

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metafsm"
	"github.com/WuKongIM/WuKongIM/pkg/storage/raftstorage"
	"github.com/stretchr/testify/require"
)

func TestMemoryBackedGroupAppliesProposalToWKDB(t *testing.T) {
	ctx := context.Background()
	groupID := multiraft.GroupID(51)
	db := openTestDB(t)
	rt := newStartedRuntime(t)

	if err := rt.BootstrapGroup(ctx, multiraft.BootstrapGroupRequest{
		Group: multiraft.GroupOptions{
			ID:           groupID,
			Storage:      raftstorage.NewMemory(),
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

	fut, err := rt.Propose(ctx, groupID, metafsm.EncodeUpsertUserCommand(metadb.User{
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
			Storage:      raftstorage.NewMemory(),
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

	fut, err := rt.Propose(ctx, groupID, metafsm.EncodeUpsertUserCommand(metadb.User{
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
	if _, err := db.ForSlot(uint64(groupID)).GetUser(ctx, "u1"); !errors.Is(err, metadb.ErrNotFound) {
		t.Fatalf("GetUser() after delete err = %v, want ErrNotFound", err)
	}

	reopenRT := newStartedRuntime(t)
	if err := reopenRT.OpenGroup(ctx, multiraft.GroupOptions{
		ID:           groupID,
		Storage:      raftstorage.NewMemory(),
		StateMachine: mustNewStateMachine(t, db, uint64(groupID)),
	}); err != nil {
		t.Fatalf("OpenGroup() error = %v", err)
	}

	_, err = db.ForSlot(uint64(groupID)).GetUser(ctx, "u1")
	if !errors.Is(err, metadb.ErrNotFound) {
		t.Fatalf("GetUser() after reopen err = %v, want ErrNotFound", err)
	}
}

func TestMemoryBackedGroupReopensWithRecoveredMembership(t *testing.T) {
	ctx := context.Background()
	groupID := multiraft.GroupID(52)
	db := openTestDB(t)
	store := raftstorage.NewMemory()

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

	fut, err := rt.Propose(ctx, groupID, metafsm.EncodeUpsertUserCommand(metadb.User{
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

	fut, err = reopenRT.Propose(ctx, groupID, metafsm.EncodeUpsertUserCommand(metadb.User{
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

	fut, err := rt.Propose(ctx, groupID, metafsm.EncodeUpsertUserCommand(metadb.User{
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

	fut, err = reopenRT.Propose(ctx, groupID, metafsm.EncodeUpsertUserCommand(metadb.User{
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

func TestStoreUpsertAndGetChannelRuntimeMeta(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	bizDB := openTestDBAt(t, filepath.Join(root, "biz"))
	raftDB := openTestRaftDBAt(t, filepath.Join(root, "raft"))

	cluster, err := raftcluster.NewCluster(raftcluster.Config{
		NodeID:     1,
		ListenAddr: "127.0.0.1:0",
		GroupCount: 1,
		NewStorage: func(groupID multiraft.GroupID) (multiraft.Storage, error) {
			return raftDB.ForGroup(uint64(groupID)), nil
		},
		NewStateMachine: metafsm.NewStateMachineFactory(bizDB),
		Nodes:           []raftcluster.NodeConfig{{NodeID: 1, Addr: "127.0.0.1:0"}},
		Groups: []raftcluster.GroupConfig{{
			GroupID: 1,
			Peers:   []multiraft.NodeID{1},
		}},
	})
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}
	t.Cleanup(cluster.Stop)
	if err := cluster.Start(); err != nil {
		t.Fatalf("cluster.Start() error = %v", err)
	}

	waitForCondition(t, func() bool {
		_, err := cluster.LeaderOf(1)
		return err == nil
	}, "cluster leader elected")

	store := New(cluster, bizDB)
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    "store-meta",
		ChannelType:  9,
		ChannelEpoch: 11,
		LeaderEpoch:  6,
		Replicas:     []uint64{3, 1, 2},
		ISR:          []uint64{2, 1},
		Leader:       1,
		MinISR:       2,
		Status:       5,
		Features:     33,
		LeaseUntilMS: 1700000004321,
	}

	if err := store.UpsertChannelRuntimeMeta(ctx, meta); err != nil {
		t.Fatalf("UpsertChannelRuntimeMeta() error = %v", err)
	}

	got, err := store.GetChannelRuntimeMeta(ctx, meta.ChannelID, meta.ChannelType)
	if err != nil {
		t.Fatalf("GetChannelRuntimeMeta() error = %v", err)
	}

	want := meta
	want.Replicas = []uint64{1, 2, 3}
	want.ISR = []uint64{1, 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stored runtime meta = %#v, want %#v", got, want)
	}
}

func TestStoreCreateUserAndUpsertDevice(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	bizDB := openTestDBAt(t, filepath.Join(root, "biz"))
	raftDB := openTestRaftDBAt(t, filepath.Join(root, "raft"))

	cluster, err := raftcluster.NewCluster(raftcluster.Config{
		NodeID:     1,
		ListenAddr: "127.0.0.1:0",
		GroupCount: 1,
		NewStorage: func(groupID multiraft.GroupID) (multiraft.Storage, error) {
			return raftDB.ForGroup(uint64(groupID)), nil
		},
		NewStateMachine: metafsm.NewStateMachineFactory(bizDB),
		Nodes:           []raftcluster.NodeConfig{{NodeID: 1, Addr: "127.0.0.1:0"}},
		Groups: []raftcluster.GroupConfig{{
			GroupID: 1,
			Peers:   []multiraft.NodeID{1},
		}},
	})
	require.NoError(t, err)
	t.Cleanup(cluster.Stop)
	require.NoError(t, cluster.Start())

	waitForCondition(t, func() bool {
		_, err := cluster.LeaderOf(1)
		return err == nil
	}, "cluster leader elected")

	store := New(cluster, bizDB)
	require.NoError(t, store.CreateUser(ctx, metadb.User{UID: "u1"}))

	gotUser, err := store.GetUser(ctx, "u1")
	require.NoError(t, err)
	require.Equal(t, "u1", gotUser.UID)

	err = store.CreateUser(ctx, metadb.User{UID: "u1", Token: "overwrite-attempt"})
	require.ErrorIs(t, err, metadb.ErrAlreadyExists)

	require.NoError(t, store.UpsertDevice(ctx, metadb.Device{
		UID:         "u1",
		DeviceFlag:  2,
		Token:       "web-token",
		DeviceLevel: 1,
	}))

	gotDevice, err := store.GetDevice(ctx, "u1", 2)
	require.NoError(t, err)
	require.Equal(t, metadb.Device{
		UID:         "u1",
		DeviceFlag:  2,
		Token:       "web-token",
		DeviceLevel: 1,
	}, gotDevice)
}

func TestStoreListChannelRuntimeMeta(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	bizDB := openTestDBAt(t, filepath.Join(root, "biz"))
	raftDB := openTestRaftDBAt(t, filepath.Join(root, "raft"))

	cluster, err := raftcluster.NewCluster(raftcluster.Config{
		NodeID:     1,
		ListenAddr: "127.0.0.1:0",
		GroupCount: 2,
		NewStorage: func(groupID multiraft.GroupID) (multiraft.Storage, error) {
			return raftDB.ForGroup(uint64(groupID)), nil
		},
		NewStateMachine: metafsm.NewStateMachineFactory(bizDB),
		Nodes:           []raftcluster.NodeConfig{{NodeID: 1, Addr: "127.0.0.1:0"}},
		Groups: []raftcluster.GroupConfig{
			{GroupID: 1, Peers: []multiraft.NodeID{1}},
			{GroupID: 2, Peers: []multiraft.NodeID{1}},
		},
	})
	require.NoError(t, err)
	t.Cleanup(cluster.Stop)
	require.NoError(t, cluster.Start())

	waitForCondition(t, func() bool {
		_, err1 := cluster.LeaderOf(1)
		_, err2 := cluster.LeaderOf(2)
		return err1 == nil && err2 == nil
	}, "cluster leader elected")

	store := New(cluster, bizDB)
	first := metadb.ChannelRuntimeMeta{
		ChannelID:    "store-list-1",
		ChannelType:  1,
		ChannelEpoch: 11,
		LeaderEpoch:  6,
		Replicas:     []uint64{1, 2},
		ISR:          []uint64{1, 2},
		Leader:       1,
		MinISR:       1,
		Status:       2,
		Features:     1,
		LeaseUntilMS: 1700000000111,
	}
	second := metadb.ChannelRuntimeMeta{
		ChannelID:    "store-list-2",
		ChannelType:  1,
		ChannelEpoch: 12,
		LeaderEpoch:  7,
		Replicas:     []uint64{1, 3},
		ISR:          []uint64{1, 3},
		Leader:       1,
		MinISR:       1,
		Status:       2,
		Features:     2,
		LeaseUntilMS: 1700000000222,
	}

	require.NoError(t, store.UpsertChannelRuntimeMeta(ctx, first))
	require.NoError(t, store.UpsertChannelRuntimeMeta(ctx, second))

	got, err := store.ListChannelRuntimeMeta(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []metadb.ChannelRuntimeMeta{first, second}, got)
}

func TestStoreGetChannelRuntimeMetaReadsAuthoritativeRemoteSlot(t *testing.T) {
	ctx := context.Background()
	nodes := startTwoNodeShardedStores(t)

	channelID := findChannelIDForSlot(t, nodes[0].cluster, 2, "remote-runtime")
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    channelID,
		ChannelType:  1,
		ChannelEpoch: 21,
		LeaderEpoch:  8,
		Replicas:     []uint64{1, 2},
		ISR:          []uint64{1, 2},
		Leader:       2,
		MinISR:       1,
		Status:       2,
		Features:     7,
		LeaseUntilMS: 1700000000999,
	}
	require.NoError(t, nodes[1].db.ForSlot(2).UpsertChannelRuntimeMeta(ctx, meta))

	got, err := nodes[0].store.GetChannelRuntimeMeta(ctx, meta.ChannelID, meta.ChannelType)
	require.NoError(t, err)
	require.Equal(t, meta, got)
}

func TestStoreGetUserReadsAuthoritativeRemoteSlot(t *testing.T) {
	ctx := context.Background()
	nodes := startTwoNodeShardedStores(t)

	uid := findUIDForSlot(t, nodes[0].cluster, 2, "remote-user")
	require.NoError(t, nodes[1].db.ForSlot(2).CreateUser(ctx, metadb.User{
		UID:         uid,
		Token:       "remote-token",
		DeviceFlag:  3,
		DeviceLevel: 7,
	}))

	got, err := nodes[0].store.GetUser(ctx, uid)
	require.NoError(t, err)
	require.Equal(t, metadb.User{
		UID:         uid,
		Token:       "remote-token",
		DeviceFlag:  3,
		DeviceLevel: 7,
	}, got)
}

func TestStoreGetDeviceReadsAuthoritativeRemoteSlot(t *testing.T) {
	ctx := context.Background()
	nodes := startTwoNodeShardedStores(t)

	uid := findUIDForSlot(t, nodes[0].cluster, 2, "remote-device")
	require.NoError(t, nodes[1].db.ForSlot(2).UpsertDevice(ctx, metadb.Device{
		UID:         uid,
		DeviceFlag:  5,
		Token:       "device-token",
		DeviceLevel: 1,
	}))

	got, err := nodes[0].store.GetDevice(ctx, uid, 5)
	require.NoError(t, err)
	require.Equal(t, metadb.Device{
		UID:         uid,
		DeviceFlag:  5,
		Token:       "device-token",
		DeviceLevel: 1,
	}, got)
}

func TestStoreCreateUserReturnsAlreadyExistsForAuthoritativeRemoteSlot(t *testing.T) {
	ctx := context.Background()
	nodes := startTwoNodeShardedStores(t)

	uid := findUIDForSlot(t, nodes[0].cluster, 2, "remote-create")
	require.NoError(t, nodes[1].db.ForSlot(2).CreateUser(ctx, metadb.User{UID: uid}))

	err := nodes[0].store.CreateUser(ctx, metadb.User{UID: uid})
	require.ErrorIs(t, err, metadb.ErrAlreadyExists)
}

func TestStoreListChannelRuntimeMetaReadsAuthoritativeAllSlots(t *testing.T) {
	ctx := context.Background()
	nodes := startTwoNodeShardedStores(t)

	first := metadb.ChannelRuntimeMeta{
		ChannelID:    findChannelIDForSlot(t, nodes[0].cluster, 1, "slot-one"),
		ChannelType:  1,
		ChannelEpoch: 31,
		LeaderEpoch:  11,
		Replicas:     []uint64{1},
		ISR:          []uint64{1},
		Leader:       1,
		MinISR:       1,
		Status:       2,
		Features:     1,
		LeaseUntilMS: 1700000001111,
	}
	second := metadb.ChannelRuntimeMeta{
		ChannelID:    findChannelIDForSlot(t, nodes[0].cluster, 2, "slot-two"),
		ChannelType:  1,
		ChannelEpoch: 32,
		LeaderEpoch:  12,
		Replicas:     []uint64{2},
		ISR:          []uint64{2},
		Leader:       2,
		MinISR:       1,
		Status:       2,
		Features:     2,
		LeaseUntilMS: 1700000002222,
	}
	require.NoError(t, nodes[0].db.ForSlot(1).UpsertChannelRuntimeMeta(ctx, first))
	require.NoError(t, nodes[1].db.ForSlot(2).UpsertChannelRuntimeMeta(ctx, second))

	got, err := nodes[0].store.ListChannelRuntimeMeta(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []metadb.ChannelRuntimeMeta{first, second}, got)
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

	fut, err := rt.Propose(ctx, groupID, metafsm.EncodeUpsertUserCommand(metadb.User{
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
	if _, err := reopenedBizDB.ForSlot(uint64(groupID)).GetUser(ctx, "u1"); !errors.Is(err, metadb.ErrNotFound) {
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

	if _, err := reopenedBizDB.ForSlot(uint64(groupID)).GetUser(ctx, "u1"); !errors.Is(err, metadb.ErrNotFound) {
		t.Fatalf("GetUser() after reopen err = %v, want ErrNotFound", err)
	}
}
