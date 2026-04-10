package isr

import (
	"context"
	"sync"
	"time"
)

type appendGroupCommitConfig struct {
	maxWait    time.Duration
	maxRecords int
	maxBytes   int
}

type appendCompletion struct {
	result CommitResult
	err    error
}

type appendRequest struct {
	ctx       context.Context
	batch     []Record
	byteCount int
	waiter    *appendWaiter
}

type replica struct {
	mu sync.RWMutex

	advanceMu sync.Mutex
	appendMu  sync.Mutex

	localNode NodeID

	log         LogStore
	checkpoints CheckpointStore
	applyFetch  ApplyFetchStore
	history     EpochHistoryStore
	snapshots   SnapshotApplier
	now         func() time.Time

	meta         ChannelMeta
	state        ReplicaState
	progress     map[NodeID]uint64
	waiters      []*appendWaiter
	epochHistory []EpochPoint
	recovered    bool

	appendGroupCommit appendGroupCommitConfig
	appendPending     []*appendRequest
	appendCollecting  bool
	appendSignal      chan struct{}
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
		applyFetch:  cfg.ApplyFetchStore,
		history:     cfg.EpochHistoryStore,
		snapshots:   cfg.SnapshotApplier,
		now:         cfg.Now,
		appendGroupCommit: appendGroupCommitConfig{
			maxWait:    effectiveAppendGroupCommitMaxWait(cfg.AppendGroupCommitMaxWait),
			maxRecords: effectiveAppendGroupCommitMaxRecords(cfg.AppendGroupCommitMaxRecords),
			maxBytes:   effectiveAppendGroupCommitMaxBytes(cfg.AppendGroupCommitMaxBytes),
		},
		appendSignal: make(chan struct{}, 1),
		state: ReplicaState{
			Role: RoleFollower,
		},
	}
	if err := r.recoverFromStores(); err != nil {
		return nil, err
	}
	return r, nil
}

func effectiveAppendGroupCommitMaxWait(configured time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return time.Millisecond
}

func effectiveAppendGroupCommitMaxRecords(configured int) int {
	if configured > 0 {
		return configured
	}
	return 64
}

func effectiveAppendGroupCommitMaxBytes(configured int) int {
	if configured > 0 {
		return configured
	}
	return 64 * 1024
}

func (r *replica) ApplyMeta(meta ChannelMeta) error {
	r.appendMu.Lock()
	defer r.appendMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state.Role == RoleTombstoned {
		return ErrTombstoned
	}
	return r.applyMetaLocked(meta)
}

func (r *replica) BecomeLeader(meta ChannelMeta) error {
	r.appendMu.Lock()
	defer r.appendMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state.Role == RoleTombstoned {
		return ErrTombstoned
	}
	if !r.recovered {
		return ErrCorruptState
	}

	normalized, err := normalizeMeta(meta)
	if err != nil {
		return err
	}
	if normalized.Leader != r.localNode {
		return ErrInvalidMeta
	}
	if err := r.validateMetaLocked(normalized); err != nil {
		return err
	}

	recoveryCutoff := r.state.HW
	leo := r.log.LEO()
	if leo < recoveryCutoff {
		return ErrCorruptState
	}
	if leo > recoveryCutoff {
		if err := r.truncateLogToLocked(recoveryCutoff); err != nil {
			return err
		}
		leo = recoveryCutoff
	}

	if len(r.epochHistory) == 0 || r.epochHistory[len(r.epochHistory)-1].Epoch != normalized.Epoch {
		point := EpochPoint{Epoch: normalized.Epoch, StartOffset: leo}
		if err := r.appendEpochPointLocked(point); err != nil {
			return err
		}
	}

	r.commitMetaLocked(normalized)
	r.state.Role = RoleLeader
	r.state.LEO = leo
	r.state.OffsetEpoch = offsetEpochForLEO(r.epochHistory, leo)
	r.seedLeaderProgressLocked(normalized.ISR, leo, recoveryCutoff)
	if !r.now().Before(normalized.LeaseUntil) {
		r.state.Role = RoleFencedLeader
		return ErrLeaseExpired
	}
	return nil
}

func (r *replica) BecomeFollower(meta ChannelMeta) error {
	r.appendMu.Lock()
	defer r.appendMu.Unlock()
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
	r.appendMu.Lock()
	defer r.appendMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.state.Role = RoleTombstoned
	return nil
}

func (r *replica) Status() ReplicaState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}
