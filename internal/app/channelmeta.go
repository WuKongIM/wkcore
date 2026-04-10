package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
	channellog "github.com/WuKongIM/WuKongIM/pkg/channel/log"
	isrnode "github.com/WuKongIM/WuKongIM/pkg/channel/node"
	isrnodetransport "github.com/WuKongIM/WuKongIM/pkg/channel/transport"
	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
	metadb "github.com/WuKongIM/WuKongIM/pkg/slot/meta"
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
	values map[isr.ChannelKey]uint64
}

type channelReplicaFactory struct {
	mu        sync.RWMutex
	db        *channellog.DB
	localNode isr.NodeID
	now       func() time.Time
	keys      map[isr.ChannelKey]channellog.ChannelKey
}

type channelLogRuntimeAdapter struct {
	runtime isrnode.Runtime
}

type channelLogChannelHandleAdapter struct {
	handle isrnode.ChannelHandle
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
		values: make(map[isr.ChannelKey]uint64),
	}
}

func (s *memoryGenerationStore) Load(channelKey isr.ChannelKey) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.values[channelKey], nil
}

func (s *memoryGenerationStore) Store(channelKey isr.ChannelKey, generation uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[channelKey] = generation
	return nil
}

func newChannelReplicaFactory(db *channellog.DB, localNode isr.NodeID, now func() time.Time) *channelReplicaFactory {
	return &channelReplicaFactory{
		db:        db,
		localNode: localNode,
		now:       now,
		keys:      make(map[isr.ChannelKey]channellog.ChannelKey),
	}
}

func (f *channelReplicaFactory) Register(key channellog.ChannelKey) {
	if f == nil {
		return
	}
	channelKey := channellog.ISRChannelKeyForChannel(key)
	f.mu.Lock()
	f.keys[channelKey] = key
	f.mu.Unlock()
}

func (f *channelReplicaFactory) New(cfg isrnode.ChannelConfig) (isr.Replica, error) {
	f.mu.RLock()
	key, ok := f.keys[cfg.ChannelKey]
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
		err = errors.Join(err, s.cleanupAppliedLocal())
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

func (a *channelLogRuntimeAdapter) Channel(channelKey isr.ChannelKey) (channellog.ChannelHandle, bool) {
	if a == nil || a.runtime == nil {
		return nil, false
	}
	handle, ok := a.runtime.Channel(channelKey)
	if !ok {
		return nil, false
	}
	return &channelLogChannelHandleAdapter{handle: handle}, true
}

func (a *channelLogChannelHandleAdapter) Append(ctx context.Context, records []isr.Record) (isr.CommitResult, error) {
	return a.handle.Append(ctx, records)
}

func (a *channelLogChannelHandleAdapter) Status() isr.ReplicaState {
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
	runtimeMeta := projectISRChannelMeta(key, meta)
	handle, runtimeExists := s.runtime.Channel(runtimeMeta.ChannelKey)
	hadAppliedLocal := s.hasAppliedLocalKey(key)
	var previousMeta isr.ChannelMeta
	if runtimeExists {
		previousMeta = handle.Meta()
	}
	if runtimeExists {
		if err := s.runtime.ApplyMeta(runtimeMeta); err != nil {
			return channellog.ChannelMeta{}, err
		}
	} else {
		if err := s.runtime.EnsureChannel(runtimeMeta); err != nil {
			return channellog.ChannelMeta{}, err
		}
	}

	s.storeAppliedLocalKey(key)
	channelMeta := projectChannelMeta(meta)
	if err := s.cluster.ApplyMeta(channelMeta); err != nil {
		if rollbackErr := s.rollbackClusterApplyFailure(key, previousMeta, runtimeExists, hadAppliedLocal); rollbackErr != nil {
			return channellog.ChannelMeta{}, errors.Join(err, rollbackErr)
		}
		return channellog.ChannelMeta{}, err
	}
	return channelMeta, nil
}

func projectISRChannelMeta(key channellog.ChannelKey, meta metadb.ChannelRuntimeMeta) isr.ChannelMeta {
	var leaseUntil time.Time
	if meta.LeaseUntilMS > 0 {
		leaseUntil = time.UnixMilli(meta.LeaseUntilMS).UTC()
	}
	return isr.ChannelMeta{
		ChannelKey: channellog.ISRChannelKeyForChannel(key),
		Epoch:      meta.LeaderEpoch,
		Leader:     isr.NodeID(meta.Leader),
		Replicas:   projectNodeIDs(meta.Replicas),
		ISR:        projectNodeIDs(meta.ISR),
		MinISR:     int(meta.MinISR),
		LeaseUntil: leaseUntil,
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

func (s *channelMetaSync) hasAppliedLocalKey(key channellog.ChannelKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.appliedLocal[key]
	return ok
}

func (s *channelMetaSync) deleteAppliedLocalKey(key channellog.ChannelKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.appliedLocal) == 0 {
		return
	}
	delete(s.appliedLocal, key)
	if len(s.appliedLocal) == 0 {
		s.appliedLocal = nil
	}
}

func (s *channelMetaSync) rollbackClusterApplyFailure(key channellog.ChannelKey, previousMeta isr.ChannelMeta, runtimeExists, hadAppliedLocal bool) error {
	var err error
	if runtimeExists {
		err = s.runtime.ApplyMeta(previousMeta)
	} else {
		err = s.removeLocal(key)
	}
	if err != nil {
		return err
	}
	if !hadAppliedLocal {
		s.deleteAppliedLocalKey(key)
	}
	return nil
}

func (s *channelMetaSync) removeLocal(key channellog.ChannelKey) error {
	channelKey := channellog.ISRChannelKeyForChannel(key)
	if err := s.runtime.RemoveChannel(channelKey); err != nil && !errors.Is(err, isrnode.ErrChannelNotFound) {
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
