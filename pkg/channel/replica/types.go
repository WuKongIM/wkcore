package replica

import (
	"context"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

type SnapshotApplier interface {
	InstallSnapshot(ctx context.Context, snap channel.Snapshot) error
}

type LogStore interface {
	LEO() uint64
	Append(records []channel.Record) (base uint64, err error)
	Read(from uint64, maxBytes int) ([]channel.Record, error)
	Truncate(to uint64) error
	Sync() error
}

type CheckpointStore interface {
	Load() (channel.Checkpoint, error)
	Store(channel.Checkpoint) error
}

type ApplyFetchStore interface {
	StoreApplyFetch(req channel.ApplyFetchStoreRequest) (leo uint64, err error)
}

type EpochHistoryStore interface {
	Load() ([]channel.EpochPoint, error)
	Append(point channel.EpochPoint) error
	TruncateTo(leo uint64) error
}

type ReplicaConfig struct {
	LocalNode                   channel.NodeID
	LogStore                    LogStore
	CheckpointStore             CheckpointStore
	ApplyFetchStore             ApplyFetchStore
	EpochHistoryStore           EpochHistoryStore
	SnapshotApplier             SnapshotApplier
	Now                         func() time.Time
	AppendGroupCommitMaxWait    time.Duration
	AppendGroupCommitMaxRecords int
	AppendGroupCommitMaxBytes   int
}

type Replica interface {
	ApplyMeta(meta channel.Meta) error
	BecomeLeader(meta channel.Meta) error
	BecomeFollower(meta channel.Meta) error
	Tombstone() error
	Append(ctx context.Context, batch []channel.Record) (channel.CommitResult, error)
	Status() channel.ReplicaState
}
