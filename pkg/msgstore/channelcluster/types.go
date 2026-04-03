package channelcluster

import (
	"context"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/consensus/isr"
)

type NodeID = isr.NodeID
type MessageSeq uint64

type ChannelStatus uint8

const (
	ChannelStatusCreating ChannelStatus = iota + 1
	ChannelStatusActive
	ChannelStatusDeleting
	ChannelStatusDeleted
)

type MessageSeqFormat uint8

const (
	MessageSeqFormatLegacyU32 MessageSeqFormat = iota + 1
	MessageSeqFormatU64
)

type ChannelFeatures struct {
	MessageSeqFormat MessageSeqFormat
}

type ChannelKey struct {
	ChannelID   string
	ChannelType uint8
}

type ChannelMeta struct {
	GroupID      uint64
	ChannelID    string
	ChannelType  uint8
	ChannelEpoch uint64
	LeaderEpoch  uint64
	Replicas     []NodeID
	ISR          []NodeID
	Leader       NodeID
	MinISR       int
	Status       ChannelStatus
	Features     ChannelFeatures
}

type SendRequest struct {
	ChannelID             string
	ChannelType           uint8
	SenderUID             string
	ClientMsgNo           string
	Payload               []byte
	SupportsMessageSeqU64 bool
	ExpectedChannelEpoch  uint64
	ExpectedLeaderEpoch   uint64
}

type SendResult struct {
	MessageID  uint64
	MessageSeq uint64
}

type FetchRequest struct {
	Key                  ChannelKey
	FromSeq              uint64
	Limit                int
	MaxBytes             int
	ExpectedChannelEpoch uint64
	ExpectedLeaderEpoch  uint64
}

type ChannelMessage struct {
	MessageID   uint64
	MessageSeq  uint64
	SenderUID   string
	ClientMsgNo string
	Payload     []byte
}

type FetchResult struct {
	Messages     []ChannelMessage
	NextSeq      uint64
	CommittedSeq uint64
}

type ChannelRuntimeStatus struct {
	Key          ChannelKey
	Status       ChannelStatus
	Leader       NodeID
	LeaderEpoch  uint64
	HW           uint64
	CommittedSeq uint64
}

type IdempotencyKey struct {
	ChannelID   string
	ChannelType uint8
	SenderUID   string
	ClientMsgNo string
}

type IdempotencyEntry struct {
	MessageID  uint64
	MessageSeq uint64
	Offset     uint64
}

type LogRecord struct {
	Offset  uint64
	Payload []byte
}

type MessageLog interface {
	Read(groupID uint64, fromOffset uint64, limit int, maxBytes int) ([]LogRecord, error)
}

type ChannelStateStore interface {
	PutIdempotency(key IdempotencyKey, entry IdempotencyEntry) error
	GetIdempotency(key IdempotencyKey) (IdempotencyEntry, bool, error)
	Snapshot(offset uint64) ([]byte, error)
	Restore(snapshot []byte) error
}

type StateStoreFactory interface {
	ForChannel(key ChannelKey) (ChannelStateStore, error)
}

type MessageIDGenerator interface {
	Next() uint64
}

type Runtime interface {
	Group(groupID uint64) (GroupHandle, bool)
}

type GroupHandle interface {
	Append(ctx context.Context, records []isr.Record) (isr.CommitResult, error)
	Status() isr.ReplicaState
}

type Config struct {
	Runtime    Runtime
	Log        MessageLog
	States     StateStoreFactory
	MessageIDs MessageIDGenerator
	Now        func() time.Time
}

type Cluster interface {
	ApplyMeta(meta ChannelMeta) error
	Send(ctx context.Context, req SendRequest) (SendResult, error)
	Fetch(ctx context.Context, req FetchRequest) (FetchResult, error)
	Status(key ChannelKey) (ChannelRuntimeStatus, error)
}
