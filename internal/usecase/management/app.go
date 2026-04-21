package management

import (
	"context"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
)

// ClusterReader exposes the cluster reads needed by manager queries.
type ClusterReader interface {
	// ListNodesStrict returns the controller leader's node snapshot without local fallback.
	ListNodesStrict(ctx context.Context) ([]controllermeta.ClusterNode, error)
	// ListSlotAssignmentsStrict returns the controller leader's slot assignments without local fallback.
	ListSlotAssignmentsStrict(ctx context.Context) ([]controllermeta.SlotAssignment, error)
	// ListObservedRuntimeViewsStrict returns the controller leader's runtime views without local fallback.
	ListObservedRuntimeViewsStrict(ctx context.Context) ([]controllermeta.SlotRuntimeView, error)
	ControllerLeaderID() uint64
}

// Options configures the management usecase app.
type Options struct {
	// LocalNodeID is the node ID of the current process.
	LocalNodeID uint64
	// ControllerPeerIDs lists the configured controller peer node IDs.
	ControllerPeerIDs []uint64
	// Cluster provides distributed cluster read access.
	Cluster ClusterReader
}

// App serves manager-oriented read usecases.
type App struct {
	localNodeID       uint64
	controllerPeerIDs map[uint64]struct{}
	cluster           ClusterReader
}

// New constructs the management usecase app.
func New(opts Options) *App {
	peers := make(map[uint64]struct{}, len(opts.ControllerPeerIDs))
	for _, nodeID := range opts.ControllerPeerIDs {
		if nodeID == 0 {
			continue
		}
		peers[nodeID] = struct{}{}
	}
	return &App{
		localNodeID:       opts.LocalNodeID,
		controllerPeerIDs: peers,
		cluster:           opts.Cluster,
	}
}
