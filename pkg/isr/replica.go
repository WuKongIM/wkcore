package isr

import (
	"context"
	"sync"
	"time"
)

type replica struct {
	mu sync.RWMutex

	localNode NodeID

	log         LogStore
	checkpoints CheckpointStore
	history     EpochHistoryStore
	snapshots   SnapshotApplier
	now         func() time.Time

	meta   GroupMeta
	state  ReplicaState
	closed bool
}

func NewReplica(cfg ReplicaConfig) (Replica, error) {
	if cfg.LocalNode == 0 {
		return nil, ErrInvalidConfig
	}
	if cfg.LogStore == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.CheckpointStore == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.EpochHistoryStore == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.SnapshotApplier == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	r := &replica{
		localNode:   cfg.LocalNode,
		log:         cfg.LogStore,
		checkpoints: cfg.CheckpointStore,
		history:     cfg.EpochHistoryStore,
		snapshots:   cfg.SnapshotApplier,
		now:         cfg.Now,
		state: ReplicaState{
			Role: RoleFollower,
		},
	}
	return r, nil
}

func (r *replica) ApplyMeta(meta GroupMeta) error {
	return errNotImplemented
}

func (r *replica) BecomeLeader(meta GroupMeta) error {
	return errNotImplemented
}

func (r *replica) BecomeFollower(meta GroupMeta) error {
	return errNotImplemented
}

func (r *replica) Tombstone() error {
	return errNotImplemented
}

func (r *replica) InstallSnapshot(ctx context.Context, snap Snapshot) error {
	return errNotImplemented
}

func (r *replica) Append(ctx context.Context, batch []Record) (CommitResult, error) {
	return CommitResult{}, errNotImplemented
}

func (r *replica) Fetch(ctx context.Context, req FetchRequest) (FetchResult, error) {
	return FetchResult{}, errNotImplemented
}

func (r *replica) ApplyFetch(ctx context.Context, req ApplyFetchRequest) error {
	return errNotImplemented
}

func (r *replica) Status() ReplicaState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}
