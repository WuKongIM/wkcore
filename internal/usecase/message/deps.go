package message

import (
	"context"

	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/internal/runtime/sequence"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
)

type OnlineRegistry = online.Registry
type Delivery = online.Delivery
type SequenceAllocator = sequence.Allocator

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
