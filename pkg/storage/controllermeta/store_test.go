package controllermeta

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStoreAssignmentAndTaskRoundTrip(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.UpsertAssignment(ctx, GroupAssignment{
		GroupID:        7,
		DesiredPeers:   []uint64{3, 1, 2, 2},
		ConfigEpoch:    11,
		BalanceVersion: 3,
	}))
	require.NoError(t, store.UpsertTask(ctx, ReconcileTask{
		GroupID:    7,
		Kind:       TaskKindRepair,
		Step:       TaskStepAddLearner,
		SourceNode: 4,
		TargetNode: 2,
		Attempt:    1,
	}))

	assignment, err := store.GetAssignment(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3}, assignment.DesiredPeers)

	task, err := store.GetTask(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, TaskKindRepair, task.Kind)
}

func TestStoreSnapshotRoundTrip(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.UpsertControllerMembership(ctx, ControllerMembership{
		Peers: []uint64{3, 1, 2},
	}))
	require.NoError(t, store.UpsertNode(ctx, ClusterNode{
		NodeID:          1,
		Addr:            "127.0.0.1:7000",
		Status:          NodeStatusAlive,
		LastHeartbeatAt: time.Unix(11, 0),
		CapacityWeight:  1,
	}))
	require.NoError(t, store.UpsertAssignment(ctx, GroupAssignment{GroupID: 1, DesiredPeers: []uint64{1, 2, 3}, ConfigEpoch: 1}))
	require.NoError(t, store.UpsertRuntimeView(ctx, GroupRuntimeView{
		GroupID:             1,
		CurrentPeers:        []uint64{1, 2, 3},
		LeaderID:            2,
		HasQuorum:           true,
		ObservedConfigEpoch: 1,
		LastReportAt:        time.Unix(12, 0),
	}))

	snap, err := store.ExportSnapshot(ctx)
	require.NoError(t, err)
	entries, err := decodeSnapshot(snap)
	require.NoError(t, err)
	require.Len(t, entries, 4)

	restored := openTestStore(t)
	require.NoError(t, restored.ImportSnapshot(ctx, snap))
	assignment, err := restored.GetAssignment(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3}, assignment.DesiredPeers)

	membership, err := restored.GetControllerMembership(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3}, membership.Peers)

	node, err := restored.GetNode(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, time.Unix(11, 0), node.LastHeartbeatAt)
	require.Equal(t, 1, node.CapacityWeight)

	view, err := restored.GetRuntimeView(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), view.LeaderID)
	require.Equal(t, uint64(1), view.ObservedConfigEpoch)
	require.Equal(t, time.Unix(12, 0), view.LastReportAt)
}

func TestStoreListsControllerStateForPlannerQueries(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.UpsertNode(ctx, ClusterNode{
		NodeID:          2,
		Addr:            "127.0.0.1:7001",
		Status:          NodeStatusDraining,
		LastHeartbeatAt: time.Unix(19, 0),
		CapacityWeight:  2,
	}))
	require.NoError(t, store.UpsertNode(ctx, ClusterNode{
		NodeID:          1,
		Addr:            "127.0.0.1:7000",
		Status:          NodeStatusAlive,
		LastHeartbeatAt: time.Unix(20, 0),
		CapacityWeight:  1,
	}))
	require.NoError(t, store.UpsertAssignment(ctx, GroupAssignment{GroupID: 2, DesiredPeers: []uint64{2, 3, 1}, ConfigEpoch: 3}))
	require.NoError(t, store.UpsertAssignment(ctx, GroupAssignment{GroupID: 1, DesiredPeers: []uint64{1, 2, 3}, ConfigEpoch: 2}))
	require.NoError(t, store.UpsertRuntimeView(ctx, GroupRuntimeView{
		GroupID:             2,
		CurrentPeers:        []uint64{3, 2, 1},
		HasQuorum:           false,
		ObservedConfigEpoch: 3,
		LastReportAt:        time.Unix(22, 0),
	}))
	require.NoError(t, store.UpsertRuntimeView(ctx, GroupRuntimeView{
		GroupID:             1,
		CurrentPeers:        []uint64{1, 2, 3},
		HasQuorum:           true,
		ObservedConfigEpoch: 2,
		LastReportAt:        time.Unix(21, 0),
	}))
	require.NoError(t, store.UpsertTask(ctx, ReconcileTask{GroupID: 2, Kind: TaskKindRebalance, Step: TaskStepTransferLeader}))
	require.NoError(t, store.UpsertTask(ctx, ReconcileTask{GroupID: 1, Kind: TaskKindRepair, Step: TaskStepAddLearner}))

	nodes, err := store.ListNodes(ctx)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.Equal(t, uint64(1), nodes[0].NodeID)
	require.Equal(t, uint64(2), nodes[1].NodeID)
	require.Equal(t, time.Unix(20, 0), nodes[0].LastHeartbeatAt)
	require.Equal(t, 1, nodes[0].CapacityWeight)

	assignments, err := store.ListAssignments(ctx)
	require.NoError(t, err)
	require.Len(t, assignments, 2)
	require.Equal(t, uint32(1), assignments[0].GroupID)
	require.Equal(t, uint32(2), assignments[1].GroupID)
	require.Equal(t, []uint64{1, 2, 3}, assignments[1].DesiredPeers)

	views, err := store.ListRuntimeViews(ctx)
	require.NoError(t, err)
	require.Len(t, views, 2)
	require.Equal(t, uint32(1), views[0].GroupID)
	require.Equal(t, uint32(2), views[1].GroupID)
	require.Equal(t, uint64(2), views[0].ObservedConfigEpoch)
	require.Equal(t, time.Unix(21, 0), views[0].LastReportAt)
	require.Equal(t, []uint64{1, 2, 3}, views[1].CurrentPeers)

	tasks, err := store.ListTasks(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	require.Equal(t, uint32(1), tasks[0].GroupID)
	require.Equal(t, uint32(2), tasks[1].GroupID)
}

func TestStoreControllerMembershipRoundTrip(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.UpsertControllerMembership(ctx, ControllerMembership{
		Peers: []uint64{3, 1, 2, 2},
	}))

	membership, err := store.GetControllerMembership(ctx)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3}, membership.Peers)
}

func TestStoreDeleteOperations(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.UpsertNode(ctx, ClusterNode{
		NodeID:          1,
		Addr:            "127.0.0.1:7000",
		Status:          NodeStatusAlive,
		LastHeartbeatAt: time.Unix(30, 0),
	}))
	require.NoError(t, store.UpsertControllerMembership(ctx, ControllerMembership{
		Peers: []uint64{1, 2, 3},
	}))
	require.NoError(t, store.UpsertAssignment(ctx, GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1, 2, 3},
	}))
	require.NoError(t, store.UpsertRuntimeView(ctx, GroupRuntimeView{
		GroupID:      1,
		CurrentPeers: []uint64{1, 2, 3},
	}))
	require.NoError(t, store.UpsertTask(ctx, ReconcileTask{
		GroupID: 1,
		Kind:    TaskKindRepair,
		Step:    TaskStepAddLearner,
	}))

	require.NoError(t, store.DeleteNode(ctx, 1))
	require.NoError(t, store.DeleteControllerMembership(ctx))
	require.NoError(t, store.DeleteAssignment(ctx, 1))
	require.NoError(t, store.DeleteRuntimeView(ctx, 1))
	require.NoError(t, store.DeleteTask(ctx, 1))

	_, err := store.GetNode(ctx, 1)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = store.GetControllerMembership(ctx)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = store.GetAssignment(ctx, 1)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = store.GetRuntimeView(ctx, 1)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = store.GetTask(ctx, 1)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestImportSnapshotRejectsCorruptValues(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.UpsertNode(ctx, ClusterNode{
		NodeID:          1,
		Addr:            "127.0.0.1:7000",
		Status:          NodeStatusAlive,
		LastHeartbeatAt: time.Unix(40, 0),
	}))

	snap, err := store.ExportSnapshot(ctx)
	require.NoError(t, err)

	entries, err := decodeSnapshot(snap)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	entries[0].Value = []byte{recordVersion}

	restored := openTestStore(t)
	err = restored.ImportSnapshot(ctx, encodeSnapshot(entries))
	require.ErrorIs(t, err, ErrCorruptValue)

	_, err = restored.GetNode(ctx, 1)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestDecodeRejectsInvalidPersistedEnums(t *testing.T) {
	nodeValue := encodeClusterNode(ClusterNode{
		NodeID:          1,
		Addr:            "127.0.0.1:7000",
		Status:          NodeStatusAlive,
		LastHeartbeatAt: time.Unix(50, 0),
	})
	nodeValue[16] = 99
	_, err := decodeClusterNode(encodeNodeKey(1), nodeValue)
	require.ErrorIs(t, err, ErrCorruptValue)

	viewValue := encodeGroupRuntimeView(GroupRuntimeView{
		GroupID:      1,
		CurrentPeers: []uint64{1, 2, 3},
		HasQuorum:    true,
	})
	viewValue[13] = 2
	_, err = decodeGroupRuntimeView(encodeGroupKey(recordPrefixRuntimeView, 1), viewValue)
	require.ErrorIs(t, err, ErrCorruptValue)

	taskValue := encodeReconcileTask(ReconcileTask{
		GroupID: 1,
		Kind:    TaskKindRepair,
		Step:    TaskStepAddLearner,
	})
	taskValue[1] = 99
	_, err = decodeReconcileTask(encodeGroupKey(recordPrefixTask, 1), taskValue)
	require.ErrorIs(t, err, ErrCorruptValue)
}

func TestStoreCanonicalizesPeerOrdering(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.UpsertAssignment(ctx, GroupAssignment{
		GroupID:      9,
		DesiredPeers: []uint64{3, 1, 2, 2},
	}))
	require.NoError(t, store.UpsertRuntimeView(ctx, GroupRuntimeView{
		GroupID:      9,
		CurrentPeers: []uint64{5, 3, 5, 4},
	}))

	assignment, err := store.GetAssignment(ctx, 9)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3}, assignment.DesiredPeers)

	view, err := store.GetRuntimeView(ctx, 9)
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 4, 5}, view.CurrentPeers)
}

func TestStoreListMethodsReturnDeterministicOrder(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.UpsertNode(ctx, ClusterNode{NodeID: 9, Addr: "127.0.0.1:7009", CapacityWeight: 1}))
	require.NoError(t, store.UpsertNode(ctx, ClusterNode{NodeID: 3, Addr: "127.0.0.1:7003", CapacityWeight: 1}))
	require.NoError(t, store.UpsertAssignment(ctx, GroupAssignment{GroupID: 8}))
	require.NoError(t, store.UpsertAssignment(ctx, GroupAssignment{GroupID: 2}))
	require.NoError(t, store.UpsertRuntimeView(ctx, GroupRuntimeView{GroupID: 7}))
	require.NoError(t, store.UpsertRuntimeView(ctx, GroupRuntimeView{GroupID: 1}))
	require.NoError(t, store.UpsertTask(ctx, ReconcileTask{GroupID: 5}))
	require.NoError(t, store.UpsertTask(ctx, ReconcileTask{GroupID: 4}))

	nodes, err := store.ListNodes(ctx)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.Equal(t, uint64(3), nodes[0].NodeID)
	require.Equal(t, uint64(9), nodes[1].NodeID)

	assignments, err := store.ListAssignments(ctx)
	require.NoError(t, err)
	require.Len(t, assignments, 2)
	require.Equal(t, uint32(2), assignments[0].GroupID)
	require.Equal(t, uint32(8), assignments[1].GroupID)

	views, err := store.ListRuntimeViews(ctx)
	require.NoError(t, err)
	require.Len(t, views, 2)
	require.Equal(t, uint32(1), views[0].GroupID)
	require.Equal(t, uint32(7), views[1].GroupID)

	tasks, err := store.ListTasks(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	require.Equal(t, uint32(4), tasks[0].GroupID)
	require.Equal(t, uint32(5), tasks[1].GroupID)
}

func openTestStore(tb testing.TB) *Store {
	tb.Helper()

	store, err := Open(filepath.Join(tb.TempDir(), "db"))
	require.NoError(tb, err)

	tb.Cleanup(func() {
		require.NoError(tb, store.Close())
	})
	return store
}
