package runtime

import (
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/replica"
)

const runtimeShardCount = 64

type shard struct {
	mu       sync.RWMutex
	channels map[core.ChannelKey]*channel
}

type runtime struct {
	cfg                Config
	shards             [runtimeShardCount]shard
	tombstones         *tombstoneManager
	replicaFactory     ReplicaFactory
	generationStore    GenerationStore
	scheduler          *scheduler
	schedulerPopHook   func(core.ChannelKey)
	sessions           peerSessionCache
	peerRequests       peerRequestState
	snapshots          snapshotState
	snapshotThrottle   snapshotThrottle
	requestID          atomic.Uint64
	replicationRetryMu sync.Mutex
	replicationRetry   map[core.ChannelKey]map[core.NodeID]*replicationRetryState
	schedulerDrainMu   sync.Mutex
	schedulerWorker    atomic.Bool
	countMu            sync.Mutex
	channelCount       int
	cleanupStop        chan struct{}
	cleanupDone        chan struct{}
	cleanupOnce        sync.Once
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
	if cfg.Limits.MaxChannels < 0 || cfg.Limits.MaxFetchInflightPeer < 0 || cfg.Limits.MaxSnapshotInflight < 0 {
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
	if cfg.Transport == nil {
		cfg.Transport = &nopTransport{}
	}
	if cfg.PeerSessions == nil {
		cfg.PeerSessions = nopPeerSessionManager{}
	}

	r := &runtime{
		cfg:              cfg,
		tombstones:       newTombstoneManager(),
		replicaFactory:   cfg.ReplicaFactory,
		generationStore:  cfg.GenerationStore,
		scheduler:        newScheduler(),
		sessions:         newPeerSessionCache(),
		peerRequests:     newPeerRequestState(),
		snapshotThrottle: newSnapshotThrottle(cfg.Limits.MaxRecoveryBytesPerSecond, time.Sleep),
		replicationRetry: make(map[core.ChannelKey]map[core.NodeID]*replicationRetryState),
		cleanupStop:      make(chan struct{}),
		cleanupDone:      make(chan struct{}),
	}
	for i := range r.shards {
		r.shards[i].channels = make(map[core.ChannelKey]*channel)
	}
	cfg.Transport.RegisterHandler(r.handleEnvelope)
	r.startTombstoneCleanup()
	return r, nil
}

func (r *runtime) EnsureChannel(meta core.Meta) error {
	shard := r.shardFor(meta.Key)
	shard.mu.Lock()

	if _, ok := shard.channels[meta.Key]; ok {
		shard.mu.Unlock()
		return ErrChannelExists
	}

	reserved := false
	if r.cfg.Limits.MaxChannels > 0 {
		if !r.tryReserveChannelSlot() {
			shard.mu.Unlock()
			return ErrTooManyChannels
		}
		reserved = true
	}

	generation, err := r.allocateGeneration(meta.Key)
	if err != nil {
		shard.mu.Unlock()
		if reserved {
			r.releaseChannelSlot()
		}
		return err
	}
	rep, err := r.replicaFactory.New(ChannelConfig{ChannelKey: meta.Key, Generation: generation, Meta: meta})
	if err != nil {
		shard.mu.Unlock()
		if reserved {
			r.releaseChannelSlot()
		}
		return err
	}
	if err := applyReplicaMeta(rep, r.cfg.LocalNode, meta); err != nil {
		shard.mu.Unlock()
		if reserved {
			r.releaseChannelSlot()
		}
		closeErr := rep.Close()
		if closeErr != nil {
			return errors.Join(err, closeErr)
		}
		return err
	}

	ch := newChannel(meta.Key, generation, rep, meta, r.cfg.Now, r)
	shard.channels[meta.Key] = ch
	shard.mu.Unlock()

	if meta.Leader != r.cfg.LocalNode {
		r.retryReplication(meta.Key, meta.Leader, true)
	}
	return nil
}

func (r *runtime) RemoveChannel(key core.ChannelKey) error {
	shard := r.shardFor(key)
	shard.mu.Lock()
	ch, ok := shard.channels[key]
	if !ok {
		shard.mu.Unlock()
		return ErrChannelNotFound
	}
	if err := ch.replica.Tombstone(); err != nil {
		shard.mu.Unlock()
		return err
	}
	r.tombstones.add(key, ch.gen, r.cfg.Now().Add(r.cfg.Tombstones.TombstoneTTL))
	delete(shard.channels, key)
	shard.mu.Unlock()
	stopTimers(r.clearReplicationRetries(key, 0, false))
	for _, peer := range r.peerRequests.clearChannel(key) {
		r.drainPeerQueue(peer)
	}

	if r.cfg.Limits.MaxChannels > 0 {
		r.releaseChannelSlot()
	}
	return ch.replica.Close()
}

func (r *runtime) ApplyMeta(meta core.Meta) error {
	ch, ok := r.lookupChannel(meta.Key)
	if !ok {
		return ErrChannelNotFound
	}
	if shouldSkipReplicaApplyMeta(ch, r.cfg.LocalNode, meta) {
		ch.setMeta(meta)
		return nil
	}
	if err := applyReplicaMeta(ch.replica, r.cfg.LocalNode, meta); err != nil {
		return err
	}
	ch.setMeta(meta)
	r.clearInvalidPeerWork(ch, meta)
	stopTimers(r.clearStaleReplicationRetries(meta.Key, meta))
	if meta.Leader != r.cfg.LocalNode {
		r.retryReplication(meta.Key, meta.Leader, true)
	}
	return nil
}

func (r *runtime) clearInvalidPeerWork(ch *channel, meta core.Meta) {
	if ch == nil {
		return
	}
	allow := func(peer core.NodeID) bool {
		return r.isReplicationPeerValid(meta, peer)
	}
	ch.clearInvalidReplicationPeers(allow)
	for _, peer := range r.peerRequests.clearChannelInvalidPeers(meta.Key, allow) {
		r.drainPeerQueue(peer)
	}
}

func (r *runtime) Channel(key core.ChannelKey) (ChannelHandle, bool) {
	ch, ok := r.lookupChannel(key)
	if !ok {
		return nil, false
	}
	return ch, true
}

func (r *runtime) lookupChannel(key core.ChannelKey) (*channel, bool) {
	shard := r.shardFor(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	ch, ok := shard.channels[key]
	return ch, ok
}

func (r *runtime) allocateGeneration(key core.ChannelKey) (uint64, error) {
	current, err := r.generationStore.Load(key)
	if err != nil {
		return 0, err
	}
	next := current + 1
	if err := r.generationStore.Store(key, next); err != nil {
		return 0, err
	}
	return next, nil
}

func (r *runtime) shardFor(key core.ChannelKey) *shard {
	idx := shardIndex(key)
	return &r.shards[idx]
}

func shardIndex(key core.ChannelKey) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % runtimeShardCount
}

func (r *runtime) totalChannels() int {
	total := 0
	for i := range r.shards {
		r.shards[i].mu.RLock()
		total += len(r.shards[i].channels)
		r.shards[i].mu.RUnlock()
	}
	return total
}

func (r *runtime) tryReserveChannelSlot() bool {
	r.countMu.Lock()
	defer r.countMu.Unlock()

	if r.channelCount >= r.cfg.Limits.MaxChannels {
		return false
	}
	r.channelCount++
	return true
}

func (r *runtime) releaseChannelSlot() {
	r.countMu.Lock()
	defer r.countMu.Unlock()

	if r.channelCount > 0 {
		r.channelCount--
	}
}

func applyReplicaMeta(rep replica.Replica, localNode core.NodeID, meta core.Meta) error {
	state := rep.Status()

	var err error
	switch {
	case meta.Leader == localNode && state.Role != core.ReplicaRoleLeader:
		err = rep.BecomeLeader(meta)
	case meta.Leader != localNode && state.Role != core.ReplicaRoleFollower:
		err = rep.BecomeFollower(meta)
	default:
		err = rep.ApplyMeta(meta)
	}
	if err == core.ErrLeaseExpired {
		return nil
	}
	return err
}

func shouldSkipReplicaApplyMeta(ch *channel, localNode core.NodeID, next core.Meta) bool {
	if ch == nil {
		return false
	}
	current := ch.metaSnapshot()
	if !metaEqual(current, next) {
		return false
	}
	state := ch.Status()
	expectedRole := core.ReplicaRoleFollower
	if next.Leader == localNode {
		expectedRole = core.ReplicaRoleLeader
	}
	if state.Role != expectedRole {
		return false
	}
	return state.ChannelKey == next.Key && state.Epoch == next.Epoch && state.Leader == next.Leader
}

func metaEqual(a, b core.Meta) bool {
	if a.Key != b.Key || a.Epoch != b.Epoch || a.Leader != b.Leader || a.MinISR != b.MinISR || !a.LeaseUntil.Equal(b.LeaseUntil) {
		return false
	}
	if len(a.Replicas) != len(b.Replicas) || len(a.ISR) != len(b.ISR) {
		return false
	}
	for i := range a.Replicas {
		if a.Replicas[i] != b.Replicas[i] {
			return false
		}
	}
	for i := range a.ISR {
		if a.ISR[i] != b.ISR[i] {
			return false
		}
	}
	return true
}

func (r *runtime) startTombstoneCleanup() {
	interval := r.cfg.Tombstones.CleanupInterval
	if interval <= 0 {
		interval = r.cfg.Tombstones.TombstoneTTL
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		defer close(r.cleanupDone)
		for {
			select {
			case <-ticker.C:
				r.tombstones.dropExpired(r.cfg.Now())
			case <-r.cleanupStop:
				return
			}
		}
	}()
}

func (r *runtime) stopTombstoneCleanup() {
	r.cleanupOnce.Do(func() {
		close(r.cleanupStop)
		<-r.cleanupDone
	})
}

func (r *runtime) Close() error {
	r.stopTombstoneCleanup()
	stopTimers(r.clearAllReplicationRetries())

	reps := make([]replica.Replica, 0)
	for i := range r.shards {
		shard := &r.shards[i]
		shard.mu.Lock()
		for key, ch := range shard.channels {
			reps = append(reps, ch.replica)
			delete(shard.channels, key)
		}
		shard.mu.Unlock()
	}

	r.countMu.Lock()
	r.channelCount = 0
	r.countMu.Unlock()

	sessions := make([]PeerSession, 0)
	r.sessions.mu.Lock()
	for peer, session := range r.sessions.sessions {
		sessions = append(sessions, session)
		delete(r.sessions.sessions, peer)
	}
	r.sessions.mu.Unlock()

	var err error
	for _, rep := range reps {
		err = errors.Join(err, rep.Close())
	}
	for _, session := range sessions {
		err = errors.Join(err, session.Close())
	}
	return err
}
