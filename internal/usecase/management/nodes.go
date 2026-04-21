package management

import (
	"context"
	"sort"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
)

// Node is the manager-facing node DTO.
type Node struct {
	// NodeID is the node identifier.
	NodeID uint64
	// Addr is the cluster listen address of the node.
	Addr string
	// Status is the manager-facing node status string.
	Status string
	// LastHeartbeatAt is the latest controller heartbeat timestamp.
	LastHeartbeatAt time.Time
	// ControllerRole is the controller role summary for the node.
	ControllerRole string
	// SlotCount is the number of observed slot peers hosted by the node.
	SlotCount int
	// LeaderSlotCount is the number of observed slots led by the node.
	LeaderSlotCount int
	// IsLocal reports whether the node is the current process node.
	IsLocal bool
	// CapacityWeight is the configured controller capacity weight.
	CapacityWeight int
}

// ListNodes returns the manager node list DTOs ordered by node ID.
func (a *App) ListNodes(ctx context.Context) ([]Node, error) {
	if a == nil || a.cluster == nil {
		return nil, nil
	}

	clusterNodes, err := a.cluster.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	views, err := a.cluster.ListObservedRuntimeViews(ctx)
	if err != nil {
		return nil, err
	}

	slotCountByNode := make(map[uint64]int, len(clusterNodes))
	leaderSlotCountByNode := make(map[uint64]int, len(clusterNodes))
	for _, view := range views {
		for _, peer := range view.CurrentPeers {
			slotCountByNode[peer]++
		}
		if view.LeaderID != 0 {
			leaderSlotCountByNode[view.LeaderID]++
		}
	}

	controllerLeaderID := a.cluster.ControllerLeaderID()
	nodes := make([]Node, 0, len(clusterNodes))
	for _, clusterNode := range clusterNodes {
		nodes = append(nodes, Node{
			NodeID:          clusterNode.NodeID,
			Addr:            clusterNode.Addr,
			Status:          managerNodeStatus(clusterNode.Status),
			LastHeartbeatAt: clusterNode.LastHeartbeatAt,
			ControllerRole:  a.controllerRole(clusterNode.NodeID, controllerLeaderID),
			SlotCount:       slotCountByNode[clusterNode.NodeID],
			LeaderSlotCount: leaderSlotCountByNode[clusterNode.NodeID],
			IsLocal:         clusterNode.NodeID == a.localNodeID,
			CapacityWeight:  clusterNode.CapacityWeight,
		})
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeID < nodes[j].NodeID
	})
	return nodes, nil
}

func (a *App) controllerRole(nodeID, controllerLeaderID uint64) string {
	if nodeID != 0 && nodeID == controllerLeaderID {
		return "leader"
	}
	if _, ok := a.controllerPeerIDs[nodeID]; ok {
		return "follower"
	}
	return "none"
}

func managerNodeStatus(status controllermeta.NodeStatus) string {
	switch status {
	case controllermeta.NodeStatusAlive:
		return "alive"
	case controllermeta.NodeStatusSuspect:
		return "suspect"
	case controllermeta.NodeStatusDead:
		return "dead"
	case controllermeta.NodeStatusDraining:
		return "draining"
	default:
		return "unknown"
	}
}
