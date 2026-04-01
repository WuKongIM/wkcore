package service

import "time"

type Service struct {
	registry  SessionRegistry
	sequencer SequenceAllocator
	delivery  DeliveryPort
	opts      Options
}

func New(opts Options) *Service {
	if opts.Now == nil {
		opts.Now = time.Now
	}

	return &Service{
		registry:  opts.sessionRegistry(),
		sequencer: opts.sequenceAllocator(),
		delivery:  opts.deliveryPort(),
		opts:      opts,
	}
}
