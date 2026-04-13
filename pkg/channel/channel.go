package channel

import (
	"context"
	"encoding/base64"
	"strconv"
	"time"
)

type Cluster interface {
	ApplyMeta(meta Meta) error
	Append(ctx context.Context, req AppendRequest) (AppendResult, error)
	Fetch(ctx context.Context, req FetchRequest) (FetchResult, error)
	Status(id ChannelID) (ChannelRuntimeStatus, error)
}

type Service interface {
	ApplyMeta(meta Meta) error
	Append(ctx context.Context, req AppendRequest) (AppendResult, error)
	Fetch(ctx context.Context, req FetchRequest) (FetchResult, error)
	Status(id ChannelID) (ChannelRuntimeStatus, error)
}

type Runtime interface {
	UpsertMeta(meta Meta) error
	RemoveChannel(key ChannelKey) error
}

type Config struct {
	LocalNode       NodeID
	Store           any
	GenerationStore any
	MessageIDs      any
	Transport       TransportConfig
	Runtime         RuntimeConfig
	Handler         HandlerConfig
	Now             func() time.Time
}

type TransportConfig struct {
	Client             any
	RPCMux             any
	RPCTimeout         time.Duration
	MaxPendingFetchRPC int
	Build              func(TransportBuildConfig) (any, error)
}

type RuntimeLimits struct {
	MaxChannels               int
	MaxFetchInflightPeer      int
	MaxSnapshotInflight       int
	MaxRecoveryBytesPerSecond int64
}

type RuntimeTombstones struct {
	TombstoneTTL    time.Duration
	CleanupInterval time.Duration
}

type RuntimeConfig struct {
	AutoRunScheduler                 bool
	FollowerReplicationRetryInterval time.Duration
	Limits                           RuntimeLimits
	Tombstones                       RuntimeTombstones
	Build                            func(RuntimeBuildConfig) (Runtime, any, error)
}

type HandlerConfig struct {
	Build func(HandlerBuildConfig) (Service, error)
}

type TransportBuildConfig struct {
	LocalNode          NodeID
	Client             any
	RPCMux             any
	RPCTimeout         time.Duration
	MaxPendingFetchRPC int
}

type RuntimeBuildConfig struct {
	LocalNode                        NodeID
	Store                            any
	GenerationStore                  any
	Transport                        any
	AutoRunScheduler                 bool
	FollowerReplicationRetryInterval time.Duration
	Limits                           RuntimeLimits
	Tombstones                       RuntimeTombstones
	Now                              func() time.Time
}

type HandlerBuildConfig struct {
	Store      any
	Runtime    any
	MessageIDs any
}

type cluster struct {
	service Service
	runtime Runtime
}

func New(cfg Config) (Cluster, error) {
	if cfg.LocalNode == 0 {
		return nil, ErrInvalidConfig
	}
	if cfg.Store == nil || cfg.GenerationStore == nil || cfg.MessageIDs == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.Transport.Build == nil || cfg.Runtime.Build == nil || cfg.Handler.Build == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	transportValue, err := cfg.Transport.Build(TransportBuildConfig{
		LocalNode:          cfg.LocalNode,
		Client:             cfg.Transport.Client,
		RPCMux:             cfg.Transport.RPCMux,
		RPCTimeout:         cfg.Transport.RPCTimeout,
		MaxPendingFetchRPC: cfg.Transport.MaxPendingFetchRPC,
	})
	if err != nil {
		return nil, err
	}

	runtimeControl, runtimeValue, err := cfg.Runtime.Build(RuntimeBuildConfig{
		LocalNode:                        cfg.LocalNode,
		Store:                            cfg.Store,
		GenerationStore:                  cfg.GenerationStore,
		Transport:                        transportValue,
		AutoRunScheduler:                 cfg.Runtime.AutoRunScheduler,
		FollowerReplicationRetryInterval: cfg.Runtime.FollowerReplicationRetryInterval,
		Limits:                           cfg.Runtime.Limits,
		Tombstones:                       cfg.Runtime.Tombstones,
		Now:                              cfg.Now,
	})
	if err != nil {
		return nil, err
	}

	service, err := cfg.Handler.Build(HandlerBuildConfig{
		Store:      cfg.Store,
		Runtime:    runtimeValue,
		MessageIDs: cfg.MessageIDs,
	})
	if err != nil {
		return nil, err
	}

	return &cluster{service: service, runtime: runtimeControl}, nil
}

func (c *cluster) ApplyMeta(meta Meta) error {
	meta.Key = effectiveChannelKey(meta)
	if err := c.service.ApplyMeta(meta); err != nil {
		return err
	}
	if meta.Status == StatusDeleted {
		return c.runtime.RemoveChannel(meta.Key)
	}
	return c.runtime.UpsertMeta(meta)
}

func (c *cluster) Append(ctx context.Context, req AppendRequest) (AppendResult, error) {
	return c.service.Append(ctx, req)
}

func (c *cluster) Fetch(ctx context.Context, req FetchRequest) (FetchResult, error) {
	return c.service.Fetch(ctx, req)
}

func (c *cluster) Status(id ChannelID) (ChannelRuntimeStatus, error) {
	return c.service.Status(id)
}

func effectiveChannelKey(meta Meta) ChannelKey {
	if meta.Key != "" {
		return meta.Key
	}
	encodedID := base64.RawURLEncoding.EncodeToString([]byte(meta.ID.ID))
	buf := make([]byte, 0, len("channel/")+4+1+len(encodedID))
	buf = append(buf, "channel/"...)
	buf = strconv.AppendUint(buf, uint64(meta.ID.Type), 10)
	buf = append(buf, '/')
	buf = append(buf, encodedID...)
	return ChannelKey(buf)
}
