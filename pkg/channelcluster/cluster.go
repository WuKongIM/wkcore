package channelcluster

import (
	"context"
	"time"
)

type cluster struct {
	cfg Config
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
	return &cluster{cfg: cfg}, nil
}

func (c *cluster) ApplyMeta(ChannelMeta) error {
	return errNotImplemented
}

func (c *cluster) Send(context.Context, SendRequest) (SendResult, error) {
	return SendResult{}, errNotImplemented
}

func (c *cluster) Fetch(context.Context, FetchRequest) (FetchResult, error) {
	return FetchResult{}, errNotImplemented
}

func (c *cluster) Status(ChannelKey) (ChannelRuntimeStatus, error) {
	return ChannelRuntimeStatus{}, errNotImplemented
}
