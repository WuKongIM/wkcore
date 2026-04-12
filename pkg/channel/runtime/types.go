package runtime

import (
	"context"
	"errors"
	"time"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/replica"
)

var (
	ErrInvalidConfig      = core.ErrInvalidConfig
	ErrChannelNotFound    = core.ErrChannelNotFound
	ErrChannelExists      = errors.New("runtime: channel already exists")
	ErrTooManyChannels    = errors.New("runtime: too many channels")
	ErrGenerationMismatch = errors.New("runtime: generation mismatch")
	ErrBackpressured      = errors.New("runtime: backpressured")
)

type TombstonePolicy struct {
	TombstoneTTL    time.Duration
	CleanupInterval time.Duration
}

type MessageKind uint8

const (
	MessageKindFetchRequest MessageKind = iota + 1
	MessageKindFetchResponse
	MessageKindFetchFailure
	MessageKindProgressAck
	MessageKindTruncate
	MessageKindSnapshotChunk
	MessageKindAck
)

type BackpressureLevel uint8

const (
	BackpressureNone BackpressureLevel = iota
	BackpressureSoft
	BackpressureHard
)

type BackpressureState struct {
	Level           BackpressureLevel
	PendingRequests int
	PendingBytes    int64
}

type Envelope struct {
	Peer       core.NodeID
	ChannelKey core.ChannelKey
	Epoch      uint64
	Generation uint64
	RequestID  uint64
	Kind       MessageKind
	Payload    []byte

	FetchRequest  *FetchRequestEnvelope
	FetchResponse *FetchResponseEnvelope
	ProgressAck   *ProgressAckEnvelope
}

type FetchRequestEnvelope struct {
	ChannelKey  core.ChannelKey
	Epoch       uint64
	Generation  uint64
	ReplicaID   core.NodeID
	FetchOffset uint64
	OffsetEpoch uint64
	MaxBytes    int
}

type FetchResponseEnvelope struct {
	ChannelKey core.ChannelKey
	Epoch      uint64
	Generation uint64
	TruncateTo *uint64
	LeaderHW   uint64
	Records    []core.Record
}

type ProgressAckEnvelope struct {
	ChannelKey  core.ChannelKey
	Epoch       uint64
	Generation  uint64
	ReplicaID   core.NodeID
	MatchOffset uint64
}

type Limits struct {
	MaxChannels               int
	MaxFetchInflightPeer      int
	MaxSnapshotInflight       int
	MaxRecoveryBytesPerSecond int64
}

type Runtime interface {
	FetchService
	EnsureChannel(meta core.Meta) error
	RemoveChannel(key core.ChannelKey) error
	ApplyMeta(meta core.Meta) error
	Channel(key core.ChannelKey) (ChannelHandle, bool)
	Close() error
}

type FetchService interface {
	ServeFetch(ctx context.Context, req FetchRequestEnvelope) (FetchResponseEnvelope, error)
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

type Transport interface {
	Send(peer core.NodeID, env Envelope) error
	RegisterHandler(fn func(Envelope))
}

type PeerSessionManager interface {
	Session(peer core.NodeID) PeerSession
}

type PeerSession interface {
	Send(env Envelope) error
	TryBatch(env Envelope) bool
	Flush() error
	Backpressure() BackpressureState
	Close() error
}

type Config struct {
	LocalNode                        core.NodeID
	ReplicaFactory                   ReplicaFactory
	GenerationStore                  GenerationStore
	Transport                        Transport
	PeerSessions                     PeerSessionManager
	AutoRunScheduler                 bool
	FollowerReplicationRetryInterval time.Duration
	Tombstones                       TombstonePolicy
	Limits                           Limits
	Now                              func() time.Time
}
