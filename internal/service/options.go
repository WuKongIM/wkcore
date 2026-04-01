package service

import "time"

type Options struct {
	Now               func() time.Time
	Registry          SessionRegistry
	SequenceAllocator SequenceAllocator
	DeliveryPort      DeliveryPort
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
