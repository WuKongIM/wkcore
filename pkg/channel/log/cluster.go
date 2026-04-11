package log

import (
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
)

type cluster struct {
	cfg    Config
	logger wklog.Logger

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
		cfg:    cfg,
		logger: defaultLogger(cfg.Logger),
		metas:  make(map[ChannelKey]ChannelMeta),
	}, nil
}

func defaultLogger(logger wklog.Logger) wklog.Logger {
	if logger == nil {
		return wklog.NewNop()
	}
	return logger
}
