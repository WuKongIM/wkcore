package message

import (
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/internal/runtime/sequence"
)

type OnlineRegistry = online.Registry
type Delivery = online.Delivery
type SequenceAllocator = sequence.Allocator

type onlineRegistryProvider interface {
	OnlineRegistry() online.Registry
}
