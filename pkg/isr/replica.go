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
	epochs []EpochPoint
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
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state.Role == RoleTombstoned {
		return ErrTombstoned
	}
	return r.applyMetaLocked(meta)
}

func (r *replica) BecomeLeader(meta GroupMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state.Role == RoleTombstoned {
		return ErrTombstoned
	}
	return errNotImplemented
}

func (r *replica) BecomeFollower(meta GroupMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state.Role == RoleTombstoned {
		return ErrTombstoned
	}
	if meta.Leader == r.localNode {
		return ErrInvalidMeta
	}
	if err := r.applyMetaLocked(meta); err != nil {
		return err
	}
	r.state.Role = RoleFollower
	return nil
}

func (r *replica) Tombstone() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.state.Role = RoleTombstoned
	return nil
}

func (r *replica) InstallSnapshot(ctx context.Context, snap Snapshot) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.state.Role == RoleTombstoned {
		return ErrTombstoned
	}
	return errNotImplemented
}

func (r *replica) Append(ctx context.Context, batch []Record) (CommitResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.state.Role == RoleTombstoned {
		return CommitResult{}, ErrTombstoned
	}
	return CommitResult{}, errNotImplemented
}

func (r *replica) Fetch(ctx context.Context, req FetchRequest) (FetchResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.state.Role == RoleTombstoned {
		return FetchResult{}, ErrTombstoned
	}
	return FetchResult{}, errNotImplemented
}

func (r *replica) ApplyFetch(ctx context.Context, req ApplyFetchRequest) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.state.Role == RoleTombstoned {
		return ErrTombstoned
	}
	return errNotImplemented
}

func (r *replica) Status() ReplicaState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}
