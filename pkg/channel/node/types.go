package node

import (
	"context"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
)

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
	Peer       isr.NodeID
	ChannelKey isr.ChannelKey
	Epoch      uint64
	Generation uint64
	RequestID  uint64
	Kind       MessageKind
	Payload    []byte

	FetchRequest  *FetchRequestEnvelope
	FetchResponse *FetchResponseEnvelope
	ProgressAck   *ProgressAckEnvelope
}

type TombstonePolicy struct {
	TombstoneTTL time.Duration
}

type Limits struct {
	MaxChannels               int
	MaxFetchInflightPeer      int
	MaxSnapshotInflight       int
	MaxRecoveryBytesPerSecond int64
}

type ChannelHandle interface {
	ID() isr.ChannelKey
	Meta() isr.ChannelMeta
	Status() isr.ReplicaState
	Append(ctx context.Context, records []isr.Record) (isr.CommitResult, error)
}

type FetchRequestEnvelope struct {
	ChannelKey  isr.ChannelKey
	Epoch       uint64
	Generation  uint64
	ReplicaID   isr.NodeID
	FetchOffset uint64
	OffsetEpoch uint64
	MaxBytes    int
}

type FetchResponseEnvelope struct {
	ChannelKey isr.ChannelKey
	Epoch      uint64
	Generation uint64
	TruncateTo *uint64
	LeaderHW   uint64
	Records    []isr.Record
}

type FetchBatchRequestEnvelope struct {
	Items []FetchBatchRequestItem
}

type FetchBatchRequestItem struct {
	RequestID uint64
	Request   FetchRequestEnvelope
}

type FetchBatchResponseEnvelope struct {
	Items []FetchBatchResponseItem
}

type FetchBatchResponseItem struct {
	RequestID uint64
	Response  *FetchResponseEnvelope
	Error     string
}

type ProgressAckEnvelope struct {
	ChannelKey  isr.ChannelKey
	Epoch       uint64
	Generation  uint64
	ReplicaID   isr.NodeID
	MatchOffset uint64
}

type FetchService interface {
	ServeFetch(ctx context.Context, req FetchRequestEnvelope) (FetchResponseEnvelope, error)
}

type Runtime interface {
	FetchService
	EnsureChannel(meta isr.ChannelMeta) error
	RemoveChannel(channelKey isr.ChannelKey) error
	ApplyMeta(meta isr.ChannelMeta) error
	Channel(channelKey isr.ChannelKey) (ChannelHandle, bool)
}

type ChannelConfig struct {
	ChannelKey isr.ChannelKey
	Generation uint64
	Meta       isr.ChannelMeta
}

type ReplicaFactory interface {
	New(cfg ChannelConfig) (isr.Replica, error)
}

type GenerationStore interface {
	Load(channelKey isr.ChannelKey) (uint64, error)
	Store(channelKey isr.ChannelKey, generation uint64) error
}

type Transport interface {
	Send(peer isr.NodeID, env Envelope) error
	RegisterHandler(fn func(Envelope))
}

type PeerSessionManager interface {
	Session(peer isr.NodeID) PeerSession
}

type PeerSession interface {
	Send(env Envelope) error
	TryBatch(env Envelope) bool
	Flush() error
	Backpressure() BackpressureState
	Close() error
}

type Config struct {
	LocalNode                        isr.NodeID
	ReplicaFactory                   ReplicaFactory
	GenerationStore                  GenerationStore
	Transport                        Transport
	PeerSessions                     PeerSessionManager
	AutoRunScheduler                 bool
	FollowerReplicationRetryInterval time.Duration
	Limits                           Limits
	Tombstones                       TombstonePolicy
	Now                              func() time.Time
}
