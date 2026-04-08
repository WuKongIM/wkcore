package isrnode

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

const (
	defaultFetchMaxBytes                 = 1 << 20
	defaultFollowerReplicationRetryDelay = 10 * time.Millisecond
)

type runtime struct {
	cfg Config

	mu               sync.RWMutex
	groups           map[isr.GroupKey]*group
	replicationRetry map[isr.GroupKey]map[isr.NodeID]bool
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
	if cfg.FollowerReplicationRetryInterval <= 0 {
		cfg.FollowerReplicationRetryInterval = defaultFollowerReplicationRetryDelay
	}
	r := &runtime{
		cfg:              cfg,
		groups:           make(map[isr.GroupKey]*group),
		replicationRetry: make(map[isr.GroupKey]map[isr.NodeID]bool),
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

	r.dropExpiredTombstonesLocked(r.cfg.Now())

	if _, ok := r.groupLocked(meta.GroupKey); ok {
		r.mu.Unlock()
		return ErrGroupExists
	}
	if r.cfg.Limits.MaxGroups > 0 && len(r.groups) >= r.cfg.Limits.MaxGroups {
		r.mu.Unlock()
		return ErrTooManyGroups
	}

	generation, err := r.allocateGeneration(meta.GroupKey)
	if err != nil {
		r.mu.Unlock()
		return err
	}
	replica, err := r.cfg.ReplicaFactory.New(GroupConfig{
		GroupKey:   meta.GroupKey,
		Generation: generation,
		Meta:       meta,
	})
	if err != nil {
		r.mu.Unlock()
		return err
	}
	if err := applyReplicaMeta(replica, r.cfg.LocalNode, meta); err != nil {
		r.mu.Unlock()
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
	bootstrapFollower := meta.Leader != r.cfg.LocalNode
	r.mu.Unlock()
	if bootstrapFollower {
		r.retryReplication(meta.GroupKey, meta.Leader, true)
	}
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
	delete(r.replicationRetry, groupKey)
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
	if err := applyReplicaMeta(g.replica, r.cfg.LocalNode, meta); err != nil {
		return err
	}
	g.setMeta(meta)
	if meta.Leader != r.cfg.LocalNode {
		r.retryReplication(meta.GroupKey, meta.Leader, true)
	}
	return nil
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
	r.enqueueScheduler(groupKey)
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

func (r *runtime) enqueueScheduler(groupKey isr.GroupKey) {
	r.scheduler.enqueue(groupKey)
	if r.cfg.AutoRunScheduler {
		go r.runScheduler()
	}
}

func (r *runtime) scheduleFollowerReplication(groupKey isr.GroupKey, leader isr.NodeID) {
	time.AfterFunc(r.cfg.FollowerReplicationRetryInterval, func() {
		r.retryReplication(groupKey, leader, false)
		r.enqueueScheduler(groupKey)
	})
}

func (r *runtime) processReplication(groupKey isr.GroupKey) {
	r.mu.RLock()
	g, ok := r.groups[groupKey]
	r.mu.RUnlock()
	if !ok {
		return
	}

	var failedPeers []isr.NodeID
	scheduleRetry := false
	for {
		peer, ok := g.popReplicationPeer()
		if !ok {
			break
		}
		meta := g.Status()
		err := r.sendEnvelope(Envelope{
			Peer:       peer,
			GroupKey:   groupKey,
			Epoch:      meta.Epoch,
			Generation: g.generation,
			RequestID:  r.requestID.Add(1),
			Kind:       MessageKindFetchRequest,
			FetchRequest: &FetchRequestEnvelope{
				GroupKey:    groupKey,
				Epoch:       meta.Epoch,
				Generation:  g.generation,
				ReplicaID:   r.cfg.LocalNode,
				FetchOffset: meta.LEO,
				OffsetEpoch: meta.OffsetEpoch,
				MaxBytes:    defaultFetchMaxBytes,
			},
		})
		if err == nil {
			r.clearReplicationRetry(groupKey, peer)
			continue
		}
		if err != nil && !errors.Is(err, ErrBackpressured) {
			failedPeers = append(failedPeers, peer)
			if r.markReplicationRetry(groupKey, peer) {
				scheduleRetry = true
			}
		}
	}
	if len(failedPeers) > 0 {
		for _, peer := range failedPeers {
			g.enqueueReplication(peer)
			r.scheduleFollowerReplication(groupKey, peer)
		}
		g.markReplication()
		if scheduleRetry {
			r.enqueueScheduler(groupKey)
		}
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
	if err := r.sendEnvelope(env); err != nil && !errors.Is(err, ErrBackpressured) {
		r.retryReplication(env.GroupKey, env.Peer, true)
	}
}

func applyReplicaMeta(replica isr.Replica, localNode isr.NodeID, meta isr.GroupMeta) error {
	state := replica.Status()

	var err error
	switch {
	case meta.Leader == localNode && state.Role != isr.RoleLeader:
		err = replica.BecomeLeader(meta)
	case meta.Leader != localNode && state.Role != isr.RoleFollower:
		err = replica.BecomeFollower(meta)
	default:
		err = replica.ApplyMeta(meta)
	}
	if errors.Is(err, isr.ErrLeaseExpired) {
		return nil
	}
	return err
}

func (r *runtime) retryReplication(groupKey isr.GroupKey, peer isr.NodeID, schedule bool) {
	r.mu.RLock()
	g, ok := r.groups[groupKey]
	r.mu.RUnlock()
	if !ok {
		return
	}

	g.enqueueReplication(peer)
	g.markReplication()
	if schedule && r.markReplicationRetry(groupKey, peer) {
		r.enqueueScheduler(groupKey)
	}
}

func (r *runtime) markReplicationRetry(groupKey isr.GroupKey, peer isr.NodeID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	peers, ok := r.replicationRetry[groupKey]
	if !ok {
		peers = make(map[isr.NodeID]bool)
		r.replicationRetry[groupKey] = peers
	}
	if peers[peer] {
		return false
	}
	peers[peer] = true
	return true
}

func (r *runtime) clearReplicationRetry(groupKey isr.GroupKey, peer isr.NodeID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	peers, ok := r.replicationRetry[groupKey]
	if !ok {
		return
	}
	delete(peers, peer)
	if len(peers) == 0 {
		delete(r.replicationRetry, groupKey)
	}
}
