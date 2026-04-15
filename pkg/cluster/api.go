package cluster

import (
	"context"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
)

type API interface {
	Start() error
	Stop()

	NodeID() multiraft.NodeID
	IsLocal(nodeID multiraft.NodeID) bool

	SlotForKey(key string) multiraft.SlotID
	LeaderOf(slotID multiraft.SlotID) (multiraft.NodeID, error)
	Propose(ctx context.Context, slotID multiraft.SlotID, cmd []byte) error

	SlotIDs() []multiraft.SlotID
	PeersForSlot(slotID multiraft.SlotID) []multiraft.NodeID
	WaitForManagedSlotsReady(ctx context.Context) error

	ListSlotAssignments(ctx context.Context) ([]controllermeta.SlotAssignment, error)
	ListObservedRuntimeViews(ctx context.Context) ([]controllermeta.SlotRuntimeView, error)
	GetReconcileTask(ctx context.Context, slotID uint32) (controllermeta.ReconcileTask, error)
	ForceReconcile(ctx context.Context, slotID uint32) error
	MarkNodeDraining(ctx context.Context, nodeID uint64) error
	ResumeNode(ctx context.Context, nodeID uint64) error
	TransferSlotLeader(ctx context.Context, slotID uint32, nodeID multiraft.NodeID) error
	RecoverSlot(ctx context.Context, slotID uint32, strategy RecoverStrategy) error

	Server() *transport.Server
	RPCMux() *transport.RPCMux
	Discovery() Discovery
	RPCService(ctx context.Context, nodeID multiraft.NodeID, slotID multiraft.SlotID, serviceID uint8, payload []byte) ([]byte, error)
}

var _ API = (*Cluster)(nil)
