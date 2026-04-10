package node

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
)

const (
	defaultFetchMaxBytes                 = 1 << 20
	defaultFollowerReplicationRetryDelay = 10 * time.Millisecond
)

type replicationRetryState struct {
	pending      bool
	timer        *time.Timer
	timerVersion uint64
}

type runtime struct {
	cfg Config

	mu               sync.RWMutex
	groups           map[isr.GroupKey]*group
	replicationRetry map[isr.GroupKey]map[isr.NodeID]*replicationRetryState
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
		replicationRetry: make(map[isr.GroupKey]map[isr.NodeID]*replicationRetryState),
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

	r.dropExpiredTombstonesLocked(r.cfg.Now())

	g, ok := r.groupLocked(groupKey)
	if !ok {
		r.mu.Unlock()
		return ErrGroupNotFound
	}
	delete(r.groups, groupKey)
	timers := r.clearReplicationRetriesLocked(groupKey, 0, false)
	if err := g.replica.Tombstone(); err != nil {
		r.mu.Unlock()
		stopTimers(timers)
		r.mu.Lock()
		return err
	}
	r.tombstoneGroupLocked(g)
	r.mu.Unlock()
	stopTimers(timers)
	return nil
}

func (r *runtime) ApplyMeta(meta isr.GroupMeta) error {
	r.mu.RLock()
	g, ok := r.groups[meta.GroupKey]
	r.mu.RUnlock()
	if !ok {
		return ErrGroupNotFound
	}

	if shouldSkipReplicaApplyMeta(g, r.cfg.LocalNode, meta) {
		g.setMeta(meta)
		return nil
	}
	if err := applyReplicaMeta(g.replica, r.cfg.LocalNode, meta); err != nil {
		return err
	}
	g.setMeta(meta)
	stopTimers(r.clearStaleReplicationRetries(meta.GroupKey, meta))
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

func shouldSkipReplicaApplyMeta(g *group, localNode isr.NodeID, next isr.GroupMeta) bool {
	if g == nil {
		return false
	}

	current := g.metaSnapshot()
	if !groupMetaEqual(current, next) {
		return false
	}

	state := g.Status()
	expectedRole := isr.RoleFollower
	if next.Leader == localNode {
		expectedRole = isr.RoleLeader
	}
	if state.Role != expectedRole {
		return false
	}
	return state.GroupKey == next.GroupKey && state.Epoch == next.Epoch && state.Leader == next.Leader
}

func groupMetaEqual(a, b isr.GroupMeta) bool {
	return a.GroupKey == b.GroupKey &&
		a.Epoch == b.Epoch &&
		a.Leader == b.Leader &&
		a.MinISR == b.MinISR &&
		a.LeaseUntil.Equal(b.LeaseUntil) &&
		slices.Equal(a.Replicas, b.Replicas) &&
		slices.Equal(a.ISR, b.ISR)
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
	r.mu.Lock()
	g, ok := r.groups[groupKey]
	if !ok {
		r.mu.Unlock()
		return
	}
	meta := g.metaSnapshot()
	if !r.isReplicationPeerValid(meta, leader) {
		r.mu.Unlock()
		return
	}
	state := r.replicationRetryStateLocked(groupKey, leader)
	if state.timer != nil {
		r.mu.Unlock()
		return
	}
	state.timerVersion++
	version := state.timerVersion
	state.timer = time.AfterFunc(r.cfg.FollowerReplicationRetryInterval, func() {
		r.fireFollowerReplicationRetry(groupKey, leader, version)
	})
	r.mu.Unlock()
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

func (r *runtime) releaseGroupInflight(groupKey isr.GroupKey, peer isr.NodeID) {
	r.peerRequests.releaseGroup(groupKey, peer)
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

	state := r.replicationRetryStateLocked(groupKey, peer)
	if state.pending {
		return false
	}
	state.pending = true
	return true
}

func (r *runtime) clearReplicationRetry(groupKey isr.GroupKey, peer isr.NodeID) {
	r.mu.Lock()
	r.clearReplicationRetryLocked(groupKey, peer)
	r.mu.Unlock()
}

func (r *runtime) fireFollowerReplicationRetry(groupKey isr.GroupKey, peer isr.NodeID, version uint64) {
	r.mu.Lock()
	peers, ok := r.replicationRetry[groupKey]
	if !ok {
		r.mu.Unlock()
		return
	}
	state, ok := peers[peer]
	if !ok || state.timer == nil || state.timerVersion != version {
		r.mu.Unlock()
		return
	}
	state.timer = nil

	g, ok := r.groups[groupKey]
	if !ok {
		state.pending = false
		r.dropReplicationRetryStateLocked(groupKey, peer)
		r.mu.Unlock()
		return
	}
	meta := g.metaSnapshot()
	if !r.isReplicationPeerValid(meta, peer) {
		state.pending = false
		r.dropReplicationRetryStateLocked(groupKey, peer)
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	r.retryReplication(groupKey, peer, false)
	r.enqueueScheduler(groupKey)
}

func (r *runtime) clearStaleReplicationRetries(groupKey isr.GroupKey, meta isr.GroupMeta) []*time.Timer {
	r.mu.Lock()
	defer r.mu.Unlock()

	peers, ok := r.replicationRetry[groupKey]
	if !ok {
		return nil
	}

	timers := make([]*time.Timer, 0, len(peers))
	for peer, state := range peers {
		if r.isReplicationPeerValid(meta, peer) {
			continue
		}
		state.pending = false
		state.timerVersion++
		if state.timer != nil {
			timers = append(timers, state.timer)
			state.timer = nil
		}
		delete(peers, peer)
	}
	if len(peers) == 0 {
		delete(r.replicationRetry, groupKey)
	}
	return timers
}

func (r *runtime) clearReplicationRetryLocked(groupKey isr.GroupKey, peer isr.NodeID) {
	peers, ok := r.replicationRetry[groupKey]
	if !ok {
		return
	}
	state, ok := peers[peer]
	if !ok {
		return
	}
	state.pending = false
	r.dropReplicationRetryStateLocked(groupKey, peer)
}

func (r *runtime) clearReplicationRetriesLocked(groupKey isr.GroupKey, keepPeer isr.NodeID, keepMatching bool) []*time.Timer {
	peers, ok := r.replicationRetry[groupKey]
	if !ok {
		return nil
	}

	timers := make([]*time.Timer, 0, len(peers))
	for peer, state := range peers {
		if keepMatching && peer == keepPeer {
			continue
		}
		state.pending = false
		state.timerVersion++
		if state.timer != nil {
			timers = append(timers, state.timer)
			state.timer = nil
		}
		delete(peers, peer)
	}
	if len(peers) == 0 {
		delete(r.replicationRetry, groupKey)
	}
	return timers
}

func (r *runtime) replicationRetryStateLocked(groupKey isr.GroupKey, peer isr.NodeID) *replicationRetryState {
	peers, ok := r.replicationRetry[groupKey]
	if !ok {
		peers = make(map[isr.NodeID]*replicationRetryState)
		r.replicationRetry[groupKey] = peers
	}
	state, ok := peers[peer]
	if !ok {
		state = &replicationRetryState{}
		peers[peer] = state
	}
	return state
}

func (r *runtime) dropReplicationRetryStateLocked(groupKey isr.GroupKey, peer isr.NodeID) {
	peers, ok := r.replicationRetry[groupKey]
	if !ok {
		return
	}
	state, ok := peers[peer]
	if !ok || state.pending || state.timer != nil {
		return
	}
	delete(peers, peer)
	if len(peers) == 0 {
		delete(r.replicationRetry, groupKey)
	}
}

func stopTimers(timers []*time.Timer) {
	for _, timer := range timers {
		if timer != nil {
			timer.Stop()
		}
	}
}

func (r *runtime) isReplicationPeerValid(meta isr.GroupMeta, peer isr.NodeID) bool {
	if peer == 0 || peer == r.cfg.LocalNode {
		return false
	}
	if meta.Leader == r.cfg.LocalNode {
		return slices.Contains(meta.Replicas, peer)
	}
	return meta.Leader == peer
}
