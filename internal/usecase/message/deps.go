package message

import (
	"context"

	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/internal/runtime/sequence"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
)

type OnlineRegistry = online.Registry
type Delivery = online.Delivery
type SequenceAllocator = sequence.Allocator

type Endpoint struct {
	NodeID     uint64
	BootID     uint64
	SessionID  uint64
	DeviceFlag uint8
}

type RecipientDirectory interface {
	EndpointsByUID(ctx context.Context, uid string) ([]Endpoint, error)
}

type RemoteDeliveryCommand struct {
	NodeID     uint64
	UID        string
	BootID     uint64
	SessionIDs []uint64
	Frame      wkframe.Frame
}

type RemoteDelivery interface {
	DeliverRemote(ctx context.Context, cmd RemoteDeliveryCommand) error
}

type CommittedMessageDispatcher interface {
	SubmitCommitted(ctx context.Context, env CommittedMessageEnvelope) error
}

type DeliveryAck interface {
	AckRoute(ctx context.Context, cmd RouteAckCommand) error
}

type DeliveryOffline interface {
	SessionClosed(ctx context.Context, cmd SessionClosedCommand) error
}

type ChannelCluster interface {
	ApplyMeta(meta channellog.ChannelMeta) error
	Send(ctx context.Context, req channellog.SendRequest) (channellog.SendResult, error)
}

type MetaRefresher interface {
	RefreshChannelMeta(ctx context.Context, key channellog.ChannelKey) (channellog.ChannelMeta, error)
}

type onlineRegistryProvider interface {
	OnlineRegistry() online.Registry
}
