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
