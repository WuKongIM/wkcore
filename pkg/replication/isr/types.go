package isr

import (
	"context"
	"time"
)

type NodeID uint64
type GroupKey string

type Role uint8

const (
	RoleFollower Role = iota + 1
	RoleLeader
	RoleFencedLeader
	RoleTombstoned
)

type GroupMeta struct {
	GroupKey   GroupKey
	Epoch      uint64
	Leader     NodeID
	Replicas   []NodeID
	ISR        []NodeID
	MinISR     int
	LeaseUntil time.Time
}

type ReplicaState struct {
	GroupKey       GroupKey
	Role           Role
	Epoch          uint64
	OffsetEpoch    uint64
	Leader         NodeID
	LogStartOffset uint64
	HW             uint64
	LEO            uint64
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

type Snapshot struct {
	GroupKey  GroupKey
	Epoch     uint64
	EndOffset uint64
	Payload   []byte
}

type EpochPoint struct {
	Epoch       uint64
	StartOffset uint64
}

type CommitResult struct {
	BaseOffset   uint64
	NextCommitHW uint64
	RecordCount  int
}

type FetchRequest struct {
	GroupKey    GroupKey
	Epoch       uint64
	ReplicaID   NodeID
	FetchOffset uint64
	OffsetEpoch uint64
	MaxBytes    int
}

type FetchResult struct {
	Epoch      uint64
	HW         uint64
	Records    []Record
	TruncateTo *uint64
}

type ApplyFetchRequest struct {
	GroupKey   GroupKey
	Epoch      uint64
	Leader     NodeID
	TruncateTo *uint64
	Records    []Record
	LeaderHW   uint64
}

type SnapshotApplier interface {
	InstallSnapshot(ctx context.Context, snap Snapshot) error
}

type LogStore interface {
	LEO() uint64
	Append(records []Record) (base uint64, err error)
	Read(from uint64, maxBytes int) ([]Record, error)
	Truncate(to uint64) error
	Sync() error
}

type CheckpointStore interface {
	Load() (Checkpoint, error)
	Store(Checkpoint) error
}

type EpochHistoryStore interface {
	Load() ([]EpochPoint, error)
	Append(point EpochPoint) error
	TruncateTo(leo uint64) error
}

type ReplicaConfig struct {
	LocalNode         NodeID
	LogStore          LogStore
	CheckpointStore   CheckpointStore
	EpochHistoryStore EpochHistoryStore
	SnapshotApplier   SnapshotApplier
	Now               func() time.Time
}

type Replica interface {
	ApplyMeta(meta GroupMeta) error
	BecomeLeader(meta GroupMeta) error
	BecomeFollower(meta GroupMeta) error
	Tombstone() error
	InstallSnapshot(ctx context.Context, snap Snapshot) error
	Append(ctx context.Context, batch []Record) (CommitResult, error)
	Fetch(ctx context.Context, req FetchRequest) (FetchResult, error)
	ApplyFetch(ctx context.Context, req ApplyFetchRequest) error
	Status() ReplicaState
}
