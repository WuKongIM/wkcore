package node

import (
	"context"

	deliveryruntime "github.com/WuKongIM/WuKongIM/internal/runtime/delivery"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	"github.com/WuKongIM/WuKongIM/pkg/channel"
	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/codec"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
)

type Cluster interface {
	RPCMux() *transport.RPCMux
	LeaderOf(slotID multiraft.SlotID) (multiraft.NodeID, error)
	IsLocal(nodeID multiraft.NodeID) bool
	SlotForKey(key string) multiraft.SlotID
	RPCService(ctx context.Context, nodeID multiraft.NodeID, slotID multiraft.SlotID, serviceID uint8, payload []byte) ([]byte, error)
	PeersForSlot(slotID multiraft.SlotID) []multiraft.NodeID
}

type Presence interface {
	presence.Authoritative
	ApplyRouteAction(ctx context.Context, action presence.RouteAction) error
}

type DeliverySubmit interface {
	SubmitCommitted(ctx context.Context, env deliveryruntime.CommittedEnvelope) error
}

type ChannelLog interface {
	Status(id channel.ChannelID) (channel.ChannelRuntimeStatus, error)
	Fetch(ctx context.Context, req channel.FetchRequest) (channel.FetchResult, error)
	Append(ctx context.Context, req channel.AppendRequest) (channel.AppendResult, error)
}

type ChannelMetaRefresher interface {
	RefreshChannelMeta(ctx context.Context, id channel.ChannelID) (channel.Meta, error)
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
	ChannelLog       ChannelLog
	DeliverySubmit   DeliverySubmit
	DeliveryAck      DeliveryAck
	DeliveryOffline  DeliveryOffline
	ChannelMeta      ChannelMetaRefresher
	DeliveryAckIndex *deliveryruntime.AckIndex
	Codec            codec.Protocol
	Logger           wklog.Logger
}

type Adapter struct {
	cluster          Cluster
	presence         Presence
	online           online.Registry
	gatewayBootID    uint64
	localNodeID      uint64
	channelLog       ChannelLog
	deliverySubmit   DeliverySubmit
	deliveryAck      DeliveryAck
	deliveryOffline  DeliveryOffline
	channelMeta      ChannelMetaRefresher
	deliveryAckIndex *deliveryruntime.AckIndex
	codec            codec.Protocol
	logger           wklog.Logger
}

func New(opts Options) *Adapter {
	if opts.Codec == nil {
		opts.Codec = codec.New()
	}
	if opts.Logger == nil {
		opts.Logger = wklog.NewNop()
	}
	adapter := &Adapter{
		cluster:          opts.Cluster,
		presence:         opts.Presence,
		online:           opts.Online,
		gatewayBootID:    opts.GatewayBootID,
		localNodeID:      opts.LocalNodeID,
		channelLog:       opts.ChannelLog,
		deliverySubmit:   opts.DeliverySubmit,
		deliveryAck:      opts.DeliveryAck,
		deliveryOffline:  opts.DeliveryOffline,
		channelMeta:      opts.ChannelMeta,
		deliveryAckIndex: opts.DeliveryAckIndex,
		codec:            opts.Codec,
		logger:           opts.Logger,
	}
	if opts.Cluster != nil && opts.Cluster.RPCMux() != nil {
		opts.Cluster.RPCMux().Handle(presenceRPCServiceID, adapter.handlePresenceRPC)
		opts.Cluster.RPCMux().Handle(deliverySubmitRPCServiceID, adapter.handleDeliverySubmitRPC)
		opts.Cluster.RPCMux().Handle(deliveryPushRPCServiceID, adapter.handleDeliveryPushRPC)
		opts.Cluster.RPCMux().Handle(deliveryAckRPCServiceID, adapter.handleDeliveryAckRPC)
		opts.Cluster.RPCMux().Handle(deliveryOfflineRPCServiceID, adapter.handleDeliveryOfflineRPC)
		opts.Cluster.RPCMux().Handle(conversationFactsRPCServiceID, adapter.handleConversationFactsRPC)
		opts.Cluster.RPCMux().Handle(channelAppendRPCServiceID, adapter.handleChannelAppendRPC)
	}
	return adapter
}

type Client struct {
	cluster Cluster
	codec   codec.Protocol
}

func NewClient(cluster Cluster) *Client {
	return &Client{
		cluster: cluster,
		codec:   codec.New(),
	}
}

var _ Cluster = (*raftcluster.Cluster)(nil)
