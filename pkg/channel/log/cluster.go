package log

import (
	"sync"
	"time"
)

type cluster struct {
	cfg Config

	mu    sync.RWMutex
	metas map[ChannelKey]ChannelMeta
}

func New(cfg Config) (Cluster, error) {
	if cfg.Runtime == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.Log == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.States == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.MessageIDs == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &cluster{
		cfg:   cfg,
		metas: make(map[ChannelKey]ChannelMeta),
	}, nil
}
