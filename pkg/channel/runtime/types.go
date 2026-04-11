package runtime

import (
	"context"
	"errors"
	"time"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/replica"
)

var (
	ErrInvalidConfig   = core.ErrInvalidConfig
	ErrChannelNotFound = core.ErrChannelNotFound
	ErrChannelExists   = errors.New("runtime: channel already exists")
	ErrTooManyChannels = errors.New("runtime: too many channels")
)

type TombstonePolicy struct {
	TombstoneTTL time.Duration
}

type Limits struct {
	MaxChannels int
}

type Runtime interface {
	EnsureChannel(meta core.Meta) error
	RemoveChannel(key core.ChannelKey) error
	ApplyMeta(meta core.Meta) error
	Channel(key core.ChannelKey) (ChannelHandle, bool)
}

type ChannelHandle interface {
	ID() core.ChannelKey
	Meta() core.Meta
	Status() core.ReplicaState
	Append(ctx context.Context, records []core.Record) (core.CommitResult, error)
}

type ChannelConfig struct {
	ChannelKey core.ChannelKey
	Generation uint64
	Meta       core.Meta
}

type ReplicaFactory interface {
	New(cfg ChannelConfig) (replica.Replica, error)
}

type GenerationStore interface {
	Load(key core.ChannelKey) (uint64, error)
	Store(key core.ChannelKey, generation uint64) error
}

type Config struct {
	LocalNode       core.NodeID
	ReplicaFactory  ReplicaFactory
	GenerationStore GenerationStore
	Tombstones      TombstonePolicy
	Limits          Limits
	Now             func() time.Time
}
