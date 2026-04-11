package log

import (
	"context"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
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

type AppendRequest struct {
	ChannelID             string
	ChannelType           uint8
	Message               Message
	SupportsMessageSeqU64 bool
	ExpectedChannelEpoch  uint64
	ExpectedLeaderEpoch   uint64
}

type AppendResult struct {
	MessageID  uint64
	MessageSeq uint64
	Message    Message
}

type FetchRequest struct {
	Key                  ChannelKey
	FromSeq              uint64
	Limit                int
	MaxBytes             int
	ExpectedChannelEpoch uint64
	ExpectedLeaderEpoch  uint64
}

type Message struct {
	MessageID uint64
	// MessageSeq is derived from the committed log offset and is not encoded in
	// the durable record payload.
	MessageSeq uint64
	// Framer persists only durable flag bits; transport/runtime sizing fields
	// are rebuilt by the realtime view layer.
	Framer      frame.Framer
	Setting     frame.Setting
	MsgKey      string
	Expire      uint32
	ClientSeq   uint64
	ClientMsgNo string
	StreamNo    string
	StreamID    uint64
	StreamFlag  frame.StreamFlag
	Timestamp   int32
	ChannelID   string
	ChannelType uint8
	Topic       string
	FromUID     string
	Payload     []byte
}

type FetchResult struct {
	Messages     []Message
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
	FromUID     string
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
	Read(channelKey isr.ChannelKey, fromOffset uint64, limit int, maxBytes int) ([]LogRecord, error)
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
	Channel(channelKey isr.ChannelKey) (ChannelHandle, bool)
}

type ChannelHandle interface {
	Append(ctx context.Context, records []isr.Record) (isr.CommitResult, error)
	Status() isr.ReplicaState
}

type Config struct {
	Runtime    Runtime
	Log        MessageLog
	States     StateStoreFactory
	MessageIDs MessageIDGenerator
	Now        func() time.Time
	Logger     wklog.Logger
}

type Cluster interface {
	ApplyMeta(meta ChannelMeta) error
	Append(ctx context.Context, req AppendRequest) (AppendResult, error)
	Fetch(ctx context.Context, req FetchRequest) (FetchResult, error)
	Status(key ChannelKey) (ChannelRuntimeStatus, error)
}
