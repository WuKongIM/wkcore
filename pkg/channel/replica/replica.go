package replica

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

type appendGroupCommitConfig struct {
	maxWait    time.Duration
	maxRecords int
	maxBytes   int
}

type appendCompletion struct {
	result channel.CommitResult
	err    error
}

type appendRequest struct {
	ctx       context.Context
	batch     []channel.Record
	byteCount int
	waiter    *appendWaiter
}

type appendWaiter struct {
	target uint64
	result channel.CommitResult
	ch     chan appendCompletion
}

type replica struct {
	mu sync.RWMutex

	advanceMu sync.Mutex
	appendMu  sync.Mutex

	localNode channel.NodeID

	log         LogStore
	checkpoints CheckpointStore
	applyFetch  ApplyFetchStore
	history     EpochHistoryStore
	snapshots   SnapshotApplier
	now         func() time.Time

	meta         channel.Meta
	state        channel.ReplicaState
	statePointer atomic.Pointer[channel.ReplicaState]
	progress     map[channel.NodeID]uint64
	waiters      []*appendWaiter
	epochHistory []channel.EpochPoint
	recovered    bool

	appendGroupCommit appendGroupCommitConfig
	appendPending     []*appendRequest
	appendSignal      chan struct{}
	stopCh            chan struct{}
	collectorDone     chan struct{}
	closeOnce         sync.Once
}

func NewReplica(cfg ReplicaConfig) (Replica, error) {
	if cfg.LocalNode == 0 {
		return nil, channel.ErrInvalidConfig
	}
	if cfg.LogStore == nil {
		return nil, channel.ErrInvalidConfig
	}
	if cfg.CheckpointStore == nil {
		return nil, channel.ErrInvalidConfig
	}
	if cfg.EpochHistoryStore == nil {
		return nil, channel.ErrInvalidConfig
	}
	if cfg.SnapshotApplier == nil {
		return nil, channel.ErrInvalidConfig
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
		appendSignal:  make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
		collectorDone: make(chan struct{}),
		state: channel.ReplicaState{
			Role: channel.ReplicaRoleFollower,
		},
	}
	r.publishStateLocked()
	if err := r.recoverFromStores(); err != nil {
		return nil, err
	}
	r.startAppendCollector()
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

func (r *replica) ApplyMeta(meta channel.Meta) error {
	r.appendMu.Lock()
	defer r.appendMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state.Role == channel.ReplicaRoleTombstoned {
		return channel.ErrTombstoned
	}
	if err := r.applyMetaLocked(meta); err != nil {
		return err
	}
	r.publishStateLocked()
	return nil
}

func (r *replica) BecomeLeader(meta channel.Meta) error {
	r.appendMu.Lock()
	defer r.appendMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state.Role == channel.ReplicaRoleTombstoned {
		return channel.ErrTombstoned
	}
	if !r.recovered {
		return channel.ErrCorruptState
	}

	normalized, err := normalizeMeta(meta)
	if err != nil {
		return err
	}
	if normalized.Leader != r.localNode {
		return channel.ErrInvalidMeta
	}
	if err := r.validateMetaLocked(normalized); err != nil {
		return err
	}

	recoveryCutoff := r.state.HW
	leo := r.log.LEO()
	if leo < recoveryCutoff {
		return channel.ErrCorruptState
	}
	if leo > recoveryCutoff {
		if err := r.truncateLogToLocked(recoveryCutoff); err != nil {
			return err
		}
		leo = recoveryCutoff
	}

	if len(r.epochHistory) == 0 || r.epochHistory[len(r.epochHistory)-1].Epoch != normalized.Epoch {
		point := channel.EpochPoint{Epoch: normalized.Epoch, StartOffset: leo}
		if err := r.appendEpochPointLocked(point); err != nil {
			return err
		}
	}

	r.commitMetaLocked(normalized)
	r.state.Role = channel.ReplicaRoleLeader
	r.state.LEO = leo
	r.state.OffsetEpoch = offsetEpochForLEO(r.epochHistory, leo)
	r.seedLeaderProgressLocked(normalized.ISR, leo, recoveryCutoff)
	if !r.now().Before(normalized.LeaseUntil) {
		r.state.Role = channel.ReplicaRoleFencedLeader
		r.publishStateLocked()
		return channel.ErrLeaseExpired
	}
	r.publishStateLocked()
	return nil
}

func (r *replica) BecomeFollower(meta channel.Meta) error {
	r.appendMu.Lock()
	defer r.appendMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state.Role == channel.ReplicaRoleTombstoned {
		return channel.ErrTombstoned
	}
	if meta.Leader == r.localNode {
		return channel.ErrInvalidMeta
	}
	if err := r.applyMetaLocked(meta); err != nil {
		return err
	}
	r.state.Role = channel.ReplicaRoleFollower
	r.publishStateLocked()
	return nil
}

func (r *replica) Tombstone() error {
	r.appendMu.Lock()
	defer r.appendMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.state.Role = channel.ReplicaRoleTombstoned
	r.publishStateLocked()
	return nil
}

func (r *replica) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		if r.stopCh != nil {
			close(r.stopCh)
		}
	})
	if r.collectorDone != nil {
		<-r.collectorDone
	}
	return nil
}

func (r *replica) Status() channel.ReplicaState {
	state := r.statePointer.Load()
	if state == nil {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.state
	}
	return *state
}

func (r *replica) publishStateLocked() {
	snapshot := r.state
	r.statePointer.Store(&snapshot)
}
