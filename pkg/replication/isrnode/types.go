package isrnode

import (
	"context"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

type MessageKind uint8

const (
	MessageKindFetchRequest MessageKind = iota + 1
	MessageKindFetchResponse
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
	GroupKey   isr.GroupKey
	Epoch      uint64
	Generation uint64
	RequestID  uint64
	Kind       MessageKind
	Payload    []byte
}

type TombstonePolicy struct {
	TombstoneTTL time.Duration
}

type Limits struct {
	MaxGroups                 int
	MaxFetchInflightPeer      int
	MaxSnapshotInflight       int
	MaxRecoveryBytesPerSecond int64
}

type GroupHandle interface {
	ID() isr.GroupKey
	Status() isr.ReplicaState
	Append(ctx context.Context, records []isr.Record) (isr.CommitResult, error)
}

type Runtime interface {
	EnsureGroup(meta isr.GroupMeta) error
	RemoveGroup(groupKey isr.GroupKey) error
	ApplyMeta(meta isr.GroupMeta) error
	Group(groupKey isr.GroupKey) (GroupHandle, bool)
}

type GroupConfig struct {
	GroupKey   isr.GroupKey
	Generation uint64
	Meta       isr.GroupMeta
}

type ReplicaFactory interface {
	New(cfg GroupConfig) (isr.Replica, error)
}

type GenerationStore interface {
	Load(groupKey isr.GroupKey) (uint64, error)
	Store(groupKey isr.GroupKey, generation uint64) error
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
	LocalNode       isr.NodeID
	ReplicaFactory  ReplicaFactory
	GenerationStore GenerationStore
	Transport       Transport
	PeerSessions    PeerSessionManager
	Limits          Limits
	Tombstones      TombstonePolicy
	Now             func() time.Time
}
