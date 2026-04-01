package service

import "time"

type Options struct {
	Now               func() time.Time
	SequenceAllocator SequenceAllocator
	DeliveryPort      DeliveryPort
}

type SequenceAllocator interface {
	NextMessageID() int64
	NextUserSequence(uid string) uint32
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
