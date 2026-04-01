package service

import (
	"context"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

type IdentityStore interface {
	GetUser(ctx context.Context, uid string) (wkdb.User, error)
}

type ChannelStore interface {
	GetChannel(ctx context.Context, channelID string, channelType int64) (wkdb.Channel, error)
}

// ClusterPort is reserved for the cluster-facing seam and remains empty for now.
type ClusterPort interface{}

type Options struct {
	Now               func() time.Time
	Registry          SessionRegistry
	SequenceAllocator SequenceAllocator
	DeliveryPort      DeliveryPort
	IdentityStore     IdentityStore
	ChannelStore      ChannelStore
	ClusterPort       ClusterPort
}

type SequenceAllocator interface {
	NextMessageID() int64
	NextChannelSequence(channelKey string) uint32
}

func (o Options) sessionRegistry() SessionRegistry {
	if o.Registry == nil {
		return NewRegistry()
	}
	return o.Registry
}

func (o Options) sequenceAllocator() SequenceAllocator {
	if o.SequenceAllocator == nil {
		return &memorySequencer{}
	}
	return o.SequenceAllocator
}

func (o Options) deliveryPort() DeliveryPort {
	if o.DeliveryPort == nil {
		return localDelivery{}
	}
	return o.DeliveryPort
}
