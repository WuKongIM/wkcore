package service

import "time"

type Service struct {
	registry *Registry
	opts     Options
}

func New(opts Options) *Service {
	if opts.Now == nil {
		opts.Now = time.Now
	}

	return &Service{
		registry: NewRegistry(),
		opts:     opts,
	}
}
