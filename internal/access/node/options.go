package node

import (
	"context"

	deliveryruntime "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkcodec"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/transport/nodetransport"
)

type Cluster interface {
	RPCMux() *nodetransport.RPCMux
	LeaderOf(groupID multiraft.GroupID) (multiraft.NodeID, error)
	IsLocal(nodeID multiraft.NodeID) bool
	SlotForKey(key string) multiraft.GroupID
	RPCService(ctx context.Context, nodeID multiraft.NodeID, groupID multiraft.GroupID, serviceID uint8, payload []byte) ([]byte, error)
	PeersForGroup(groupID multiraft.GroupID) []multiraft.NodeID
}

type Presence interface {
	presence.Authoritative
	ApplyRouteAction(ctx context.Context, action presence.RouteAction) error
}

type DeliverySubmit interface {
	SubmitCommitted(ctx context.Context, msg channellog.Message) error
}

type DeliveryAck interface {
	AckRoute(ctx context.Context, cmd message.RouteAckCommand) error
}

type DeliveryOffline interface {
	SessionClosed(ctx context.Context, cmd message.SessionClosedCommand) error
}

type Options struct {
	Cluster          Cluster
	Presence         Presence
	Online           online.Registry
	GatewayBootID    uint64
	LocalNodeID      uint64
	DeliverySubmit   DeliverySubmit
	DeliveryAck      DeliveryAck
	DeliveryOffline  DeliveryOffline
	DeliveryAckIndex *deliveryruntime.AckIndex
	Codec            wkcodec.Protocol
}

type Adapter struct {
	cluster          Cluster
	presence         Presence
	online           online.Registry
	gatewayBootID    uint64
	localNodeID      uint64
	deliverySubmit   DeliverySubmit
	deliveryAck      DeliveryAck
	deliveryOffline  DeliveryOffline
	deliveryAckIndex *deliveryruntime.AckIndex
	codec            wkcodec.Protocol
}

func New(opts Options) *Adapter {
	if opts.Codec == nil {
		opts.Codec = wkcodec.New()
	}
	adapter := &Adapter{
		cluster:          opts.Cluster,
		presence:         opts.Presence,
		online:           opts.Online,
		gatewayBootID:    opts.GatewayBootID,
		localNodeID:      opts.LocalNodeID,
		deliverySubmit:   opts.DeliverySubmit,
		deliveryAck:      opts.DeliveryAck,
		deliveryOffline:  opts.DeliveryOffline,
		deliveryAckIndex: opts.DeliveryAckIndex,
		codec:            opts.Codec,
	}
	if opts.Cluster != nil && opts.Cluster.RPCMux() != nil {
		opts.Cluster.RPCMux().Handle(presenceRPCServiceID, adapter.handlePresenceRPC)
		opts.Cluster.RPCMux().Handle(deliverySubmitRPCServiceID, adapter.handleDeliverySubmitRPC)
		opts.Cluster.RPCMux().Handle(deliveryPushRPCServiceID, adapter.handleDeliveryPushRPC)
		opts.Cluster.RPCMux().Handle(deliveryAckRPCServiceID, adapter.handleDeliveryAckRPC)
		opts.Cluster.RPCMux().Handle(deliveryOfflineRPCServiceID, adapter.handleDeliveryOfflineRPC)
	}
	return adapter
}

type Client struct {
	cluster Cluster
	codec   wkcodec.Protocol
}

func NewClient(cluster Cluster) *Client {
	return &Client{
		cluster: cluster,
		codec:   wkcodec.New(),
	}
}

var _ Cluster = (*raftcluster.Cluster)(nil)
