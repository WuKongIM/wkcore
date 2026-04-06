package node

import (
	"context"

	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkcodec"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
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

type Options struct {
	Cluster       Cluster
	Presence      Presence
	Online        online.Registry
	GatewayBootID uint64
	Codec         wkcodec.Protocol
}

type Adapter struct {
	cluster       Cluster
	presence      Presence
	online        online.Registry
	gatewayBootID uint64
	codec         wkcodec.Protocol
}

func New(opts Options) *Adapter {
	if opts.Codec == nil {
		opts.Codec = wkcodec.New()
	}
	adapter := &Adapter{
		cluster:       opts.Cluster,
		presence:      opts.Presence,
		online:        opts.Online,
		gatewayBootID: opts.GatewayBootID,
		codec:         opts.Codec,
	}
	if opts.Cluster != nil && opts.Cluster.RPCMux() != nil {
		opts.Cluster.RPCMux().Handle(presenceRPCServiceID, adapter.handlePresenceRPC)
		opts.Cluster.RPCMux().Handle(deliveryRPCServiceID, adapter.handleDeliveryRPC)
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
