package multiisr

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/isr"
)

type runtime struct {
	cfg Config

	mu             sync.RWMutex
	groups         map[uint64]*group
	scheduler      *scheduler
	tombstones     map[uint64]map[uint64]tombstone
	sessions       peerSessionCache
	peerRequests   peerRequestState
	snapshots      snapshotState
	requestID      atomic.Uint64
	advanceClock   func(time.Duration)
	snapshotRunner func(groupID uint64, bytes int64) bool
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
		cfg:          cfg,
		groups:       make(map[uint64]*group),
		scheduler:    newScheduler(),
		sessions:     newPeerSessionCache(),
		peerRequests: newPeerRequestState(),
		tombstones:   make(map[uint64]map[uint64]tombstone),
	}
	cfg.Transport.RegisterHandler(r.handleEnvelope)
	return r, nil
}

func (r *runtime) EnsureGroup(meta isr.GroupMeta) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.dropExpiredTombstonesLocked(r.cfg.Now())

	if _, ok := r.groupLocked(meta.GroupID); ok {
		return ErrGroupExists
	}
	if r.cfg.Limits.MaxGroups > 0 && len(r.groups) >= r.cfg.Limits.MaxGroups {
		return ErrTooManyGroups
	}

	generation, err := r.allocateGeneration(meta.GroupID)
	if err != nil {
		return err
	}
	replica, err := r.cfg.ReplicaFactory.New(GroupConfig{
		GroupID:    meta.GroupID,
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
		id:         meta.GroupID,
		generation: generation,
		replica:    replica,
		now:        r.cfg.Now,
		onReplication: func() {
			r.processReplication(meta.GroupID)
		},
		onSnapshot: func() {
			r.processSnapshot(meta.GroupID)
		},
	})
	r.groups[meta.GroupID].setMeta(meta)
	return nil
}

func (r *runtime) RemoveGroup(groupID uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.dropExpiredTombstonesLocked(r.cfg.Now())

	g, ok := r.groupLocked(groupID)
	if !ok {
		return ErrGroupNotFound
	}
	delete(r.groups, groupID)
	if err := g.replica.Tombstone(); err != nil {
		return err
	}
	r.tombstoneGroupLocked(g)
	return nil
}

func (r *runtime) ApplyMeta(meta isr.GroupMeta) error {
	r.mu.RLock()
	g, ok := r.groups[meta.GroupID]
	r.mu.RUnlock()
	if !ok {
		return ErrGroupNotFound
	}
	g.setMeta(meta)
	return g.replica.ApplyMeta(meta)
}

func (r *runtime) Group(groupID uint64) (GroupHandle, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.dropExpiredTombstonesLocked(r.cfg.Now())
	g, ok := r.groupLocked(groupID)
	if !ok {
		return nil, false
	}
	return g, true
}

func (r *runtime) enqueueReplication(groupID uint64, peer isr.NodeID) {
	r.mu.RLock()
	g, ok := r.groups[groupID]
	r.mu.RUnlock()
	if !ok {
		return
	}
	g.enqueueReplication(peer)
	g.markReplication()
	r.scheduler.enqueue(groupID)
}

func (r *runtime) runScheduler() {
	for {
		select {
		case groupID := <-r.scheduler.ch:
			r.scheduler.begin(groupID)
			r.processGroup(groupID)
			if r.scheduler.done(groupID) {
				r.scheduler.requeue(groupID)
			}
		default:
			return
		}
	}
}

func (r *runtime) processGroup(groupID uint64) {
	r.mu.RLock()
	g, ok := r.groups[groupID]
	r.mu.RUnlock()
	if !ok {
		return
	}
	g.runPendingTasks()
}

func (r *runtime) processReplication(groupID uint64) {
	r.mu.RLock()
	g, ok := r.groups[groupID]
	r.mu.RUnlock()
	if !ok {
		return
	}

	meta := g.Status()
	for _, peer := range g.drainReplicationPeers() {
		_ = r.sendEnvelope(Envelope{
			Peer:       peer,
			GroupID:    groupID,
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
