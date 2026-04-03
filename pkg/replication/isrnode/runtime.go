package isrnode

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

type runtime struct {
	cfg Config

	mu               sync.RWMutex
	groups           map[isr.GroupKey]*group
	scheduler        *scheduler
	tombstones       map[isr.GroupKey]map[uint64]tombstone
	sessions         peerSessionCache
	peerRequests     peerRequestState
	snapshots        snapshotState
	snapshotThrottle snapshotThrottle
	requestID        atomic.Uint64
}

func New(cfg Config) (Runtime, error) {
	if cfg.LocalNode == 0 {
		return nil, ErrInvalidConfig
	}
	if cfg.ReplicaFactory == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.GenerationStore == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.Transport == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.PeerSessions == nil {
		return nil, ErrInvalidConfig
	}
	if cfg.Limits.MaxGroups < 0 {
		return nil, ErrInvalidConfig
	}
	if cfg.Tombstones.TombstoneTTL <= 0 {
		return nil, ErrInvalidConfig
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	r := &runtime{
		cfg:              cfg,
		groups:           make(map[isr.GroupKey]*group),
		scheduler:        newScheduler(),
		sessions:         newPeerSessionCache(),
		peerRequests:     newPeerRequestState(),
		tombstones:       make(map[isr.GroupKey]map[uint64]tombstone),
		snapshotThrottle: newSnapshotThrottle(cfg.Limits.MaxRecoveryBytesPerSecond, time.Sleep),
	}
	cfg.Transport.RegisterHandler(r.handleEnvelope)
	return r, nil
}

func (r *runtime) EnsureGroup(meta isr.GroupMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.dropExpiredTombstonesLocked(r.cfg.Now())

	if _, ok := r.groupLocked(meta.GroupKey); ok {
		return ErrGroupExists
	}
	if r.cfg.Limits.MaxGroups > 0 && len(r.groups) >= r.cfg.Limits.MaxGroups {
		return ErrTooManyGroups
	}

	generation, err := r.allocateGeneration(meta.GroupKey)
	if err != nil {
		return err
	}
	replica, err := r.cfg.ReplicaFactory.New(GroupConfig{
		GroupKey:   meta.GroupKey,
		Generation: generation,
		Meta:       meta,
	})
	if err != nil {
		return err
	}
	if err := replica.ApplyMeta(meta); err != nil {
		return err
	}

	r.putGroupLocked(&group{
		id:         meta.GroupKey,
		generation: generation,
		replica:    replica,
		now:        r.cfg.Now,
		onReplication: func() {
			r.processReplication(meta.GroupKey)
		},
		onSnapshot: func() {
			r.processSnapshot(meta.GroupKey)
		},
	})
	r.groups[meta.GroupKey].setMeta(meta)
	return nil
}

func (r *runtime) RemoveGroup(groupKey isr.GroupKey) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.dropExpiredTombstonesLocked(r.cfg.Now())

	g, ok := r.groupLocked(groupKey)
	if !ok {
		return ErrGroupNotFound
	}
	delete(r.groups, groupKey)
	if err := g.replica.Tombstone(); err != nil {
		return err
	}
	r.tombstoneGroupLocked(g)
	return nil
}

func (r *runtime) ApplyMeta(meta isr.GroupMeta) error {
	r.mu.RLock()
	g, ok := r.groups[meta.GroupKey]
	r.mu.RUnlock()
	if !ok {
		return ErrGroupNotFound
	}
	g.setMeta(meta)
	return g.replica.ApplyMeta(meta)
}

func (r *runtime) Group(groupKey isr.GroupKey) (GroupHandle, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.dropExpiredTombstonesLocked(r.cfg.Now())
	g, ok := r.groupLocked(groupKey)
	if !ok {
		return nil, false
	}
	return g, true
}

func (r *runtime) enqueueReplication(groupKey isr.GroupKey, peer isr.NodeID) {
	r.mu.RLock()
	g, ok := r.groups[groupKey]
	r.mu.RUnlock()
	if !ok {
		return
	}
	g.enqueueReplication(peer)
	g.markReplication()
	r.scheduler.enqueue(groupKey)
}

func (r *runtime) runScheduler() {
	for {
		select {
		case groupKey := <-r.scheduler.ch:
			r.scheduler.begin(groupKey)
			r.processGroup(groupKey)
			if r.scheduler.done(groupKey) {
				r.scheduler.requeue(groupKey)
			}
		default:
			return
		}
	}
}

func (r *runtime) processGroup(groupKey isr.GroupKey) {
	r.mu.RLock()
	g, ok := r.groups[groupKey]
	r.mu.RUnlock()
	if !ok {
		return
	}
	g.runPendingTasks()
}

func (r *runtime) processReplication(groupKey isr.GroupKey) {
	r.mu.RLock()
	g, ok := r.groups[groupKey]
	r.mu.RUnlock()
	if !ok {
		return
	}

	meta := g.Status()
	for {
		peer, ok := g.popReplicationPeer()
		if !ok {
			break
		}
		_ = r.sendEnvelope(Envelope{
			Peer:       peer,
			GroupKey:   groupKey,
			Epoch:      meta.Epoch,
			Generation: g.generation,
			RequestID:  r.requestID.Add(1),
			Kind:       MessageKindFetchRequest,
		})
	}
}

func (r *runtime) queuedPeerRequests(peer isr.NodeID) int {
	return r.peerRequests.queuedCount(peer)
}

func (r *runtime) releasePeerInflight(peer isr.NodeID) {
	r.peerRequests.release(peer)
}

func (r *runtime) drainPeerQueue(peer isr.NodeID) {
	env, ok := r.peerRequests.popQueued(peer)
	if !ok {
		return
	}
	_ = r.sendEnvelope(env)
}
