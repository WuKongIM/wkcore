package message

import (
	"context"

	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/internal/runtime/sequence"
	"github.com/WuKongIM/WuKongIM/pkg/msgstore/channelcluster"
)

type OnlineRegistry = online.Registry
type Delivery = online.Delivery
type SequenceAllocator = sequence.Allocator

type ChannelCluster interface {
	ApplyMeta(meta channelcluster.ChannelMeta) error
	Send(ctx context.Context, req channelcluster.SendRequest) (channelcluster.SendResult, error)
}

type MetaRefresher interface {
	RefreshChannelMeta(ctx context.Context, key channelcluster.ChannelKey) (channelcluster.ChannelMeta, error)
}

type onlineRegistryProvider interface {
	OnlineRegistry() online.Registry
}
