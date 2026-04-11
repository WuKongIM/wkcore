package node

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
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
	cfg    Config
	logger wklog.Logger

	mu               sync.RWMutex
	groups           map[isr.ChannelKey]*group
	replicationRetry map[isr.ChannelKey]map[isr.NodeID]*replicationRetryState
	scheduler        *scheduler
	tombstones       map[isr.ChannelKey]map[uint64]tombstone
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
	if cfg.Limits.MaxChannels < 0 {
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
		logger:           defaultLogger(cfg.Logger),
		groups:           make(map[isr.ChannelKey]*group),
		replicationRetry: make(map[isr.ChannelKey]map[isr.NodeID]*replicationRetryState),
		scheduler:        newScheduler(),
		sessions:         newPeerSessionCache(),
		peerRequests:     newPeerRequestState(),
		tombstones:       make(map[isr.ChannelKey]map[uint64]tombstone),
		snapshotThrottle: newSnapshotThrottle(cfg.Limits.MaxRecoveryBytesPerSecond, time.Sleep),
	}
	cfg.Transport.RegisterHandler(r.handleEnvelope)
	return r, nil
}

func defaultLogger(logger wklog.Logger) wklog.Logger {
	if logger == nil {
		return wklog.NewNop()
	}
	return logger
}

func (r *runtime) EnsureChannel(meta isr.ChannelMeta) error {
	r.mu.Lock()

	r.dropExpiredTombstonesLocked(r.cfg.Now())

	if _, ok := r.channelLocked(meta.ChannelKey); ok {
		r.mu.Unlock()
		return ErrChannelExists
	}
	if r.cfg.Limits.MaxChannels > 0 && len(r.groups) >= r.cfg.Limits.MaxChannels {
		r.mu.Unlock()
		return ErrTooManyChannels
	}

	generation, err := r.allocateGeneration(meta.ChannelKey)
	if err != nil {
		r.mu.Unlock()
		return err
	}
	replica, err := r.cfg.ReplicaFactory.New(ChannelConfig{
		ChannelKey: meta.ChannelKey,
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
		id:         meta.ChannelKey,
		generation: generation,
		replica:    replica,
		now:        r.cfg.Now,
		onReplication: func() {
			r.processReplication(meta.ChannelKey)
		},
		onSnapshot: func() {
			r.processSnapshot(meta.ChannelKey)
		},
	})
	r.groups[meta.ChannelKey].setMeta(meta)
	bootstrapFollower := meta.Leader != r.cfg.LocalNode
	r.mu.Unlock()
	if bootstrapFollower {
		r.retryReplication(meta.ChannelKey, meta.Leader, true)
	}
	return nil
}

func (r *runtime) RemoveChannel(channelKey isr.ChannelKey) error {
	r.mu.Lock()

	r.dropExpiredTombstonesLocked(r.cfg.Now())

	g, ok := r.channelLocked(channelKey)
	if !ok {
		r.mu.Unlock()
		return ErrChannelNotFound
	}
	delete(r.groups, channelKey)
	timers := r.clearReplicationRetriesLocked(channelKey, 0, false)
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

func (r *runtime) ApplyMeta(meta isr.ChannelMeta) error {
	r.mu.RLock()
	g, ok := r.groups[meta.ChannelKey]
	r.mu.RUnlock()
	if !ok {
		return ErrChannelNotFound
	}

	if shouldSkipReplicaApplyMeta(g, r.cfg.LocalNode, meta) {
		g.setMeta(meta)
		return nil
	}
	if err := applyReplicaMeta(g.replica, r.cfg.LocalNode, meta); err != nil {
		return err
	}
	g.setMeta(meta)
	stopTimers(r.clearStaleReplicationRetries(meta.ChannelKey, meta))
	if meta.Leader != r.cfg.LocalNode {
		r.retryReplication(meta.ChannelKey, meta.Leader, true)
	}
	return nil
}

func (r *runtime) Channel(channelKey isr.ChannelKey) (ChannelHandle, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.dropExpiredTombstonesLocked(r.cfg.Now())
	g, ok := r.channelLocked(channelKey)
	if !ok {
		return nil, false
	}
	return g, true
}

func (r *runtime) enqueueReplication(channelKey isr.ChannelKey, peer isr.NodeID) {
	r.mu.RLock()
	g, ok := r.groups[channelKey]
	r.mu.RUnlock()
	if !ok {
		return
	}
	g.enqueueReplication(peer)
	g.markReplication()
	r.enqueueScheduler(channelKey)
}

func shouldSkipReplicaApplyMeta(g *group, localNode isr.NodeID, next isr.ChannelMeta) bool {
	if g == nil {
		return false
	}

	current := g.metaSnapshot()
	if !channelMetaEqual(current, next) {
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
	return state.ChannelKey == next.ChannelKey && state.Epoch == next.Epoch && state.Leader == next.Leader
}

func channelMetaEqual(a, b isr.ChannelMeta) bool {
	return a.ChannelKey == b.ChannelKey &&
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
		case channelKey := <-r.scheduler.ch:
			r.scheduler.begin(channelKey)
			r.processChannel(channelKey)
			if r.scheduler.done(channelKey) {
				r.scheduler.requeue(channelKey)
			}
		default:
			return
		}
	}
}

func (r *runtime) processChannel(channelKey isr.ChannelKey) {
	r.mu.RLock()
	g, ok := r.groups[channelKey]
	r.mu.RUnlock()
	if !ok {
		return
	}
	g.runPendingTasks()
}

func (r *runtime) enqueueScheduler(channelKey isr.ChannelKey) {
	r.scheduler.enqueue(channelKey)
	if r.cfg.AutoRunScheduler {
		go r.runScheduler()
	}
}

func (r *runtime) scheduleFollowerReplication(channelKey isr.ChannelKey, leader isr.NodeID) {
	r.mu.Lock()
	g, ok := r.groups[channelKey]
	if !ok {
		r.mu.Unlock()
		return
	}
	meta := g.metaSnapshot()
	if !r.isReplicationPeerValid(meta, leader) {
		r.mu.Unlock()
		return
	}
	state := r.replicationRetryStateLocked(channelKey, leader)
	if state.timer != nil {
		r.mu.Unlock()
		return
	}
	state.timerVersion++
	version := state.timerVersion
	state.timer = time.AfterFunc(r.cfg.FollowerReplicationRetryInterval, func() {
		r.fireFollowerReplicationRetry(channelKey, leader, version)
	})
	r.mu.Unlock()
}

func (r *runtime) processReplication(channelKey isr.ChannelKey) {
	r.mu.RLock()
	g, ok := r.groups[channelKey]
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
			ChannelKey: channelKey,
			Epoch:      meta.Epoch,
			Generation: g.generation,
			RequestID:  r.requestID.Add(1),
			Kind:       MessageKindFetchRequest,
			FetchRequest: &FetchRequestEnvelope{
				ChannelKey:  channelKey,
				Epoch:       meta.Epoch,
				Generation:  g.generation,
				ReplicaID:   r.cfg.LocalNode,
				FetchOffset: meta.LEO,
				OffsetEpoch: meta.OffsetEpoch,
				MaxBytes:    defaultFetchMaxBytes,
			},
		})
		if err == nil {
			r.clearReplicationRetry(channelKey, peer)
			continue
		}
		if err != nil && !errors.Is(err, ErrBackpressured) {
			failedPeers = append(failedPeers, peer)
			if r.markReplicationRetry(channelKey, peer) {
				scheduleRetry = true
			}
		}
	}
	if len(failedPeers) > 0 {
		for _, peer := range failedPeers {
			g.enqueueReplication(peer)
			r.scheduleFollowerReplication(channelKey, peer)
		}
		g.markReplication()
		if scheduleRetry {
			r.enqueueScheduler(channelKey)
		}
	}
}

func (r *runtime) queuedPeerRequests(peer isr.NodeID) int {
	return r.peerRequests.queuedCount(peer)
}

func (r *runtime) releasePeerInflight(peer isr.NodeID) {
	r.peerRequests.release(peer)
}

func (r *runtime) releaseChannelInflight(channelKey isr.ChannelKey, peer isr.NodeID) {
	r.peerRequests.releaseChannel(channelKey, peer)
}

func (r *runtime) drainPeerQueue(peer isr.NodeID) {
	env, ok := r.peerRequests.popQueued(peer)
	if !ok {
		return
	}
	if err := r.sendEnvelope(env); err != nil && !errors.Is(err, ErrBackpressured) {
		r.retryReplication(env.ChannelKey, env.Peer, true)
	}
}

func applyReplicaMeta(replica isr.Replica, localNode isr.NodeID, meta isr.ChannelMeta) error {
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

func (r *runtime) retryReplication(channelKey isr.ChannelKey, peer isr.NodeID, schedule bool) {
	r.mu.RLock()
	g, ok := r.groups[channelKey]
	r.mu.RUnlock()
	if !ok {
		return
	}

	g.enqueueReplication(peer)
	g.markReplication()
	if schedule && r.markReplicationRetry(channelKey, peer) {
		r.enqueueScheduler(channelKey)
	}
}

func (r *runtime) markReplicationRetry(channelKey isr.ChannelKey, peer isr.NodeID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	state := r.replicationRetryStateLocked(channelKey, peer)
	if state.pending {
		return false
	}
	state.pending = true
	return true
}

func (r *runtime) clearReplicationRetry(channelKey isr.ChannelKey, peer isr.NodeID) {
	r.mu.Lock()
	r.clearReplicationRetryLocked(channelKey, peer)
	r.mu.Unlock()
}

func (r *runtime) fireFollowerReplicationRetry(channelKey isr.ChannelKey, peer isr.NodeID, version uint64) {
	r.mu.Lock()
	peers, ok := r.replicationRetry[channelKey]
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

	g, ok := r.groups[channelKey]
	if !ok {
		state.pending = false
		r.dropReplicationRetryStateLocked(channelKey, peer)
		r.mu.Unlock()
		return
	}
	meta := g.metaSnapshot()
	if !r.isReplicationPeerValid(meta, peer) {
		state.pending = false
		r.dropReplicationRetryStateLocked(channelKey, peer)
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	r.retryReplication(channelKey, peer, false)
	r.enqueueScheduler(channelKey)
}

func (r *runtime) clearStaleReplicationRetries(channelKey isr.ChannelKey, meta isr.ChannelMeta) []*time.Timer {
	r.mu.Lock()
	defer r.mu.Unlock()

	peers, ok := r.replicationRetry[channelKey]
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
		delete(r.replicationRetry, channelKey)
	}
	return timers
}

func (r *runtime) clearReplicationRetryLocked(channelKey isr.ChannelKey, peer isr.NodeID) {
	peers, ok := r.replicationRetry[channelKey]
	if !ok {
		return
	}
	state, ok := peers[peer]
	if !ok {
		return
	}
	state.pending = false
	r.dropReplicationRetryStateLocked(channelKey, peer)
}

func (r *runtime) clearReplicationRetriesLocked(channelKey isr.ChannelKey, keepPeer isr.NodeID, keepMatching bool) []*time.Timer {
	peers, ok := r.replicationRetry[channelKey]
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
		delete(r.replicationRetry, channelKey)
	}
	return timers
}

func (r *runtime) replicationRetryStateLocked(channelKey isr.ChannelKey, peer isr.NodeID) *replicationRetryState {
	peers, ok := r.replicationRetry[channelKey]
	if !ok {
		peers = make(map[isr.NodeID]*replicationRetryState)
		r.replicationRetry[channelKey] = peers
	}
	state, ok := peers[peer]
	if !ok {
		state = &replicationRetryState{}
		peers[peer] = state
	}
	return state
}

func (r *runtime) dropReplicationRetryStateLocked(channelKey isr.ChannelKey, peer isr.NodeID) {
	peers, ok := r.replicationRetry[channelKey]
	if !ok {
		return
	}
	state, ok := peers[peer]
	if !ok || state.pending || state.timer != nil {
		return
	}
	delete(peers, peer)
	if len(peers) == 0 {
		delete(r.replicationRetry, channelKey)
	}
}

func stopTimers(timers []*time.Timer) {
	for _, timer := range timers {
		if timer != nil {
			timer.Stop()
		}
	}
}

func (r *runtime) isReplicationPeerValid(meta isr.ChannelMeta, peer isr.NodeID) bool {
	if peer == 0 || peer == r.cfg.LocalNode {
		return false
	}
	if meta.Leader == r.cfg.LocalNode {
		return slices.Contains(meta.Replicas, peer)
	}
	return meta.Leader == peer
}
