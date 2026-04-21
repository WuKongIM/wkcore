package management

import (
	"context"
	"testing"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	"github.com/stretchr/testify/require"
)

func TestListNodesAggregatesControllerRoleAndSlotCounts(t *testing.T) {
	now := time.Unix(1713686400, 0).UTC()
	app := New(Options{
		LocalNodeID:       2,
		ControllerPeerIDs: []uint64{1, 2},
		Cluster: fakeClusterReader{
			controllerLeaderID: 1,
			nodes: []controllermeta.ClusterNode{
				{NodeID: 3, Addr: "127.0.0.1:7003", Status: controllermeta.NodeStatusAlive, LastHeartbeatAt: now.Add(-3 * time.Second), CapacityWeight: 1},
				{NodeID: 1, Addr: "127.0.0.1:7001", Status: controllermeta.NodeStatusAlive, LastHeartbeatAt: now.Add(-1 * time.Second), CapacityWeight: 1},
				{NodeID: 2, Addr: "127.0.0.1:7002", Status: controllermeta.NodeStatusDraining, LastHeartbeatAt: now.Add(-2 * time.Second), CapacityWeight: 2},
			},
			views: []controllermeta.SlotRuntimeView{
				{SlotID: 1, CurrentPeers: []uint64{1, 2}, LeaderID: 1, HasQuorum: true},
				{SlotID: 2, CurrentPeers: []uint64{2, 3}, LeaderID: 2, HasQuorum: true},
			},
		},
	})

	got, err := app.ListNodes(context.Background())
	require.NoError(t, err)
	require.Equal(t, []nodeSummary{
		{NodeID: 1, Status: "alive", ControllerRole: "leader", SlotCount: 1, LeaderSlotCount: 1, IsLocal: false},
		{NodeID: 2, Status: "draining", ControllerRole: "follower", SlotCount: 2, LeaderSlotCount: 1, IsLocal: true},
		{NodeID: 3, Status: "alive", ControllerRole: "none", SlotCount: 1, LeaderSlotCount: 0, IsLocal: false},
	}, summarizeNodes(got))
	require.Equal(t, now.Add(-1*time.Second), got[0].LastHeartbeatAt)
	require.Equal(t, 2, got[1].CapacityWeight)
}

func TestListNodesSortsByNodeIDAndDefaultsCountsToZero(t *testing.T) {
	app := New(Options{
		LocalNodeID:       9,
		ControllerPeerIDs: []uint64{4},
		Cluster: fakeClusterReader{
			controllerLeaderID: 4,
			nodes: []controllermeta.ClusterNode{
				{NodeID: 9, Addr: "127.0.0.1:7009", Status: controllermeta.NodeStatusSuspect, CapacityWeight: 1},
				{NodeID: 4, Addr: "127.0.0.1:7004", Status: controllermeta.NodeStatusDead, CapacityWeight: 3},
			},
		},
	})

	got, err := app.ListNodes(context.Background())
	require.NoError(t, err)
	require.Equal(t, []nodeSummary{
		{NodeID: 4, Status: "dead", ControllerRole: "leader", SlotCount: 0, LeaderSlotCount: 0, IsLocal: false},
		{NodeID: 9, Status: "suspect", ControllerRole: "none", SlotCount: 0, LeaderSlotCount: 0, IsLocal: true},
	}, summarizeNodes(got))
}

type fakeClusterReader struct {
	controllerLeaderID uint64
	nodes              []controllermeta.ClusterNode
	views              []controllermeta.SlotRuntimeView
}

func (f fakeClusterReader) ListNodes(context.Context) ([]controllermeta.ClusterNode, error) {
	return append([]controllermeta.ClusterNode(nil), f.nodes...), nil
}

func (f fakeClusterReader) ListObservedRuntimeViews(context.Context) ([]controllermeta.SlotRuntimeView, error) {
	return append([]controllermeta.SlotRuntimeView(nil), f.views...), nil
}

func (f fakeClusterReader) ControllerLeaderID() uint64 {
	return f.controllerLeaderID
}

type nodeSummary struct {
	NodeID          uint64
	Status          string
	ControllerRole  string
	SlotCount       int
	LeaderSlotCount int
	IsLocal         bool
}

func summarizeNodes(nodes []Node) []nodeSummary {
	out := make([]nodeSummary, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, nodeSummary{
			NodeID:          node.NodeID,
			Status:          node.Status,
			ControllerRole:  node.ControllerRole,
			SlotCount:       node.SlotCount,
			LeaderSlotCount: node.LeaderSlotCount,
			IsLocal:         node.IsLocal,
		})
	}
	return out
}
