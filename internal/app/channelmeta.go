package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isrnode"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isrnodetransport"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
)

var errChannelDataPlaneNotReady = fmt.Errorf("app: channel data plane not ready")

type channelMetaSource interface {
	GetChannelRuntimeMeta(ctx context.Context, channelID string, channelType int64) (metadb.ChannelRuntimeMeta, error)
	ListChannelRuntimeMeta(ctx context.Context) ([]metadb.ChannelRuntimeMeta, error)
}

type isrTransportBridge struct {
	mu      sync.RWMutex
	handler func(isrnode.Envelope)
	adapter *isrnodetransport.Adapter
}

type noopPeerSession struct{}

type memoryGenerationStore struct {
	mu     sync.Mutex
	values map[isr.GroupKey]uint64
}

type channelReplicaFactory struct {
	mu        sync.RWMutex
	db        *channellog.DB
	localNode isr.NodeID
	now       func() time.Time
	keys      map[isr.GroupKey]channellog.ChannelKey
}

type channelLogRuntimeAdapter struct {
	runtime isrnode.Runtime
}

type channelLogGroupHandleAdapter struct {
	handle isrnode.GroupHandle
}

type removableChannelMeta interface {
	RemoveMeta(key channellog.ChannelKey) error
}

type channelMetaSync struct {
	source          channelMetaSource
	runtime         isrnode.Runtime
	cluster         channellog.Cluster
	replicaFactory  *channelReplicaFactory
	localNode       uint64
	refreshInterval time.Duration

	mu           sync.Mutex
	cancel       context.CancelFunc
	done         chan struct{}
	appliedLocal map[channellog.ChannelKey]struct{}
}

func newISRTransportBridge() *isrTransportBridge {
	return &isrTransportBridge{}
}

func (t *isrTransportBridge) Send(peer isr.NodeID, env isrnode.Envelope) error {
	t.mu.RLock()
	adapter := t.adapter
	t.mu.RUnlock()
	if adapter == nil {
		return errChannelDataPlaneNotReady
	}
	return adapter.Send(peer, env)
}

func (t *isrTransportBridge) RegisterHandler(fn func(isrnode.Envelope)) {
	t.mu.Lock()
	t.handler = fn
	adapter := t.adapter
	t.mu.Unlock()
	if adapter != nil {
		adapter.RegisterHandler(fn)
	}
}

func (t *isrTransportBridge) Session(peer isr.NodeID) isrnode.PeerSession {
	t.mu.RLock()
	adapter := t.adapter
	t.mu.RUnlock()
	if adapter != nil {
		return adapter.SessionManager().Session(peer)
	}
	return noopPeerSession{}
}

func (noopPeerSession) Send(isrnode.Envelope) error {
	return errChannelDataPlaneNotReady
}

func (noopPeerSession) TryBatch(isrnode.Envelope) bool {
	return false
}

func (noopPeerSession) Flush() error {
	return nil
}

func (noopPeerSession) Backpressure() isrnode.BackpressureState {
	return isrnode.BackpressureState{}
}

func (noopPeerSession) Close() error {
	return nil
}

func newMemoryGenerationStore() *memoryGenerationStore {
	return &memoryGenerationStore{
		values: make(map[isr.GroupKey]uint64),
	}
}

func (s *memoryGenerationStore) Load(groupKey isr.GroupKey) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[groupKey], nil
}

func (s *memoryGenerationStore) Store(groupKey isr.GroupKey, generation uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[groupKey] = generation
	return nil
}

func newChannelReplicaFactory(db *channellog.DB, localNode isr.NodeID, now func() time.Time) *channelReplicaFactory {
	return &channelReplicaFactory{
		db:        db,
		localNode: localNode,
		now:       now,
		keys:      make(map[isr.GroupKey]channellog.ChannelKey),
	}
}

func (f *channelReplicaFactory) Register(key channellog.ChannelKey) {
	if f == nil {
		return
	}
	groupKey := channellog.GroupKeyForChannel(key)
	f.mu.Lock()
	f.keys[groupKey] = key
	f.mu.Unlock()
}

func (f *channelReplicaFactory) New(cfg isrnode.GroupConfig) (isr.Replica, error) {
	f.mu.RLock()
	key, ok := f.keys[cfg.GroupKey]
	f.mu.RUnlock()
	if !ok {
		return nil, errChannelDataPlaneNotReady
	}
	return channellog.NewReplica(f.db.ForChannel(key), f.localNode, f.now)
}

func (s *channelMetaSync) RefreshChannelMeta(ctx context.Context, key channellog.ChannelKey) (channellog.ChannelMeta, error) {
	meta, err := s.source.GetChannelRuntimeMeta(ctx, key.ChannelID, int64(key.ChannelType))
	if err != nil {
		return channellog.ChannelMeta{}, err
	}
	return s.apply(meta)
}

func (s *channelMetaSync) Start() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	interval := s.refreshInterval
	if interval <= 0 {
		interval = time.Second
	}
	s.cancel = cancel
	s.done = done
	s.mu.Unlock()

	err := s.syncOnce(ctx)
	if errors.Is(err, raftcluster.ErrNoLeader) {
		err = nil
	}
	if err != nil {
		cancel()
		s.mu.Lock()
		s.cancel = nil
		s.done = nil
		s.mu.Unlock()
		return err
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = s.syncOnce(ctx)
			}
		}
	}()
	return nil
}

func (s *channelMetaSync) Stop() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	cancel := s.cancel
	done := s.done
	s.cancel = nil
	s.done = nil
	s.mu.Unlock()
	if cancel == nil {
		return s.cleanupAppliedLocal()
	}
	cancel()
	<-done
	return s.cleanupAppliedLocal()
}

func (s *channelMetaSync) syncOnce(ctx context.Context) error {
	metas, err := s.source.ListChannelRuntimeMeta(ctx)
	if err != nil {
		return err
	}
	currentLocal := make(map[channellog.ChannelKey]struct{})
	for _, meta := range metas {
		if !containsUint64(meta.Replicas, s.localNode) {
			continue
		}
		key := channellog.ChannelKey{
			ChannelID:   meta.ChannelID,
			ChannelType: uint8(meta.ChannelType),
		}
		if _, err := s.apply(meta); err != nil {
			return err
		}
		currentLocal[key] = struct{}{}
	}
	for key := range s.snapshotAppliedLocal() {
		if _, ok := currentLocal[key]; ok {
			continue
		}
		if err := s.removeLocal(key); err != nil {
			return err
		}
	}
	s.replaceAppliedLocal(currentLocal)
	return nil
}

func newChannelLogRuntimeAdapter(runtime isrnode.Runtime) channellog.Runtime {
	return &channelLogRuntimeAdapter{runtime: runtime}
}

func (a *channelLogRuntimeAdapter) Group(groupKey isr.GroupKey) (channellog.GroupHandle, bool) {
	if a == nil || a.runtime == nil {
		return nil, false
	}
	handle, ok := a.runtime.Group(groupKey)
	if !ok {
		return nil, false
	}
	return &channelLogGroupHandleAdapter{handle: handle}, true
}

func (a *channelLogGroupHandleAdapter) Append(ctx context.Context, records []isr.Record) (isr.CommitResult, error) {
	return a.handle.Append(ctx, records)
}

func (a *channelLogGroupHandleAdapter) Status() isr.ReplicaState {
	return a.handle.Status()
}

func (s *channelMetaSync) apply(meta metadb.ChannelRuntimeMeta) (channellog.ChannelMeta, error) {
	key := channellog.ChannelKey{
		ChannelID:   meta.ChannelID,
		ChannelType: uint8(meta.ChannelType),
	}
	if !containsUint64(meta.Replicas, s.localNode) {
		return channellog.ChannelMeta{}, channellog.ErrStaleMeta
	}
	if s.replicaFactory != nil {
		s.replicaFactory.Register(key)
	}
	runtimeMeta := projectISRGroupMeta(key, meta)
	if _, ok := s.runtime.Group(runtimeMeta.GroupKey); ok {
		if err := s.runtime.ApplyMeta(runtimeMeta); err != nil {
			return channellog.ChannelMeta{}, err
		}
	} else {
		if err := s.runtime.EnsureGroup(runtimeMeta); err != nil {
			return channellog.ChannelMeta{}, err
		}
	}

	s.storeAppliedLocalKey(key)
	channelMeta := projectChannelMeta(meta)
	if err := s.cluster.ApplyMeta(channelMeta); err != nil {
		return channellog.ChannelMeta{}, err
	}
	return channelMeta, nil
}

func projectISRGroupMeta(key channellog.ChannelKey, meta metadb.ChannelRuntimeMeta) isr.GroupMeta {
	return isr.GroupMeta{
		GroupKey:   channellog.GroupKeyForChannel(key),
		Epoch:      meta.LeaderEpoch,
		Leader:     isr.NodeID(meta.Leader),
		Replicas:   projectNodeIDs(meta.Replicas),
		ISR:        projectNodeIDs(meta.ISR),
		MinISR:     int(meta.MinISR),
		LeaseUntil: time.UnixMilli(meta.LeaseUntilMS).UTC(),
	}
}

func projectChannelMeta(meta metadb.ChannelRuntimeMeta) channellog.ChannelMeta {
	return channellog.ChannelMeta{
		ChannelID:    meta.ChannelID,
		ChannelType:  uint8(meta.ChannelType),
		ChannelEpoch: meta.ChannelEpoch,
		LeaderEpoch:  meta.LeaderEpoch,
		Replicas:     projectChannelNodeIDs(meta.Replicas),
		ISR:          projectChannelNodeIDs(meta.ISR),
		Leader:       channellog.NodeID(meta.Leader),
		MinISR:       int(meta.MinISR),
		Status:       channellog.ChannelStatus(meta.Status),
		Features: channellog.ChannelFeatures{
			MessageSeqFormat: channellog.MessageSeqFormat(meta.Features),
		},
	}
}

func projectNodeIDs(ids []uint64) []isr.NodeID {
	out := make([]isr.NodeID, 0, len(ids))
	for _, id := range ids {
		out = append(out, isr.NodeID(id))
	}
	return out
}

func projectChannelNodeIDs(ids []uint64) []channellog.NodeID {
	out := make([]channellog.NodeID, 0, len(ids))
	for _, id := range ids {
		out = append(out, channellog.NodeID(id))
	}
	return out
}

func containsUint64(values []uint64, target uint64) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func (s *channelMetaSync) snapshotAppliedLocal() map[channellog.ChannelKey]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneAppliedLocalSet(s.appliedLocal)
}

func (s *channelMetaSync) replaceAppliedLocal(values map[channellog.ChannelKey]struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appliedLocal = cloneAppliedLocalSet(values)
}

func (s *channelMetaSync) storeAppliedLocalKey(key channellog.ChannelKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.appliedLocal == nil {
		s.appliedLocal = make(map[channellog.ChannelKey]struct{})
	}
	s.appliedLocal[key] = struct{}{}
}

func (s *channelMetaSync) removeLocal(key channellog.ChannelKey) error {
	groupKey := channellog.GroupKeyForChannel(key)
	if err := s.runtime.RemoveGroup(groupKey); err != nil && !errors.Is(err, isrnode.ErrGroupNotFound) {
		return err
	}
	if cluster, ok := s.cluster.(removableChannelMeta); ok {
		if err := cluster.RemoveMeta(key); err != nil {
			return err
		}
	}
	return nil
}

func (s *channelMetaSync) cleanupAppliedLocal() error {
	var err error
	for key := range s.snapshotAppliedLocal() {
		err = errors.Join(err, s.removeLocal(key))
	}
	s.replaceAppliedLocal(nil)
	return err
}

func cloneAppliedLocalSet(values map[channellog.ChannelKey]struct{}) map[channellog.ChannelKey]struct{} {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[channellog.ChannelKey]struct{}, len(values))
	for key := range values {
		cloned[key] = struct{}{}
	}
	return cloned
}

func (t *isrTransportBridge) Bind(adapter *isrnodetransport.Adapter) {
	t.mu.Lock()
	t.adapter = adapter
	handler := t.handler
	t.mu.Unlock()
	if adapter != nil && handler != nil {
		adapter.RegisterHandler(handler)
	}
}

func (t *isrTransportBridge) Unbind() {
	t.mu.Lock()
	t.adapter = nil
	t.mu.Unlock()
}
