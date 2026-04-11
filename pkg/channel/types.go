package channel

import "time"

type NodeID uint64
type ChannelKey string

type ChannelID struct {
	ID   string
	Type uint8
}

type Role uint8
type Status uint8
type MessageSeqFormat uint8

const (
	StatusCreating Status = iota + 1
	StatusActive
	StatusDeleting
	StatusDeleted
)

const (
	MessageSeqFormatLegacyU32 MessageSeqFormat = iota + 1
	MessageSeqFormatU64
)

type Features struct {
	MessageSeqFormat MessageSeqFormat
}

type Message struct {
	MessageID   uint64
	MessageSeq  uint64
	ChannelID   string
	ChannelType uint8
	FromUID     string
	Payload     []byte
}

type Meta struct {
	Key         ChannelKey
	ID          ChannelID
	Epoch       uint64
	LeaderEpoch uint64
	Leader      NodeID
	Replicas    []NodeID
	ISR         []NodeID
	MinISR      int
	LeaseUntil  time.Time
	Status      Status
	Features    Features
}

type AppendRequest struct {
	ChannelID            ChannelID
	Message              Message
	ExpectedChannelEpoch uint64
	ExpectedLeaderEpoch  uint64
}

type AppendResult struct {
	MessageID  uint64
	MessageSeq uint64
	Message    Message
}

type FetchRequest struct {
	ChannelID            ChannelID
	FromSeq              uint64
	Limit                int
	MaxBytes             int
	ExpectedChannelEpoch uint64
	ExpectedLeaderEpoch  uint64
}

type FetchResult struct {
	Messages     []Message
	NextSeq      uint64
	CommittedSeq uint64
}

type Record struct {
	Payload   []byte
	SizeBytes int
}

type Checkpoint struct {
	Epoch          uint64
	LogStartOffset uint64
	HW             uint64
}

type EpochPoint struct {
	Epoch       uint64
	StartOffset uint64
}

type Snapshot struct {
	ChannelKey ChannelKey
	Epoch      uint64
	EndOffset  uint64
	Payload    []byte
}

type IdempotencyKey struct {
	ChannelID   ChannelID
	FromUID     string
	ClientMsgNo string
}

type IdempotencyEntry struct {
	MessageID  uint64
	MessageSeq uint64
	Offset     uint64
}

type ApplyFetchStoreRequest struct {
	Records    []Record
	Checkpoint *Checkpoint
}
