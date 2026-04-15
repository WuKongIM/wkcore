package app

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	channelhandler "github.com/WuKongIM/WuKongIM/pkg/channel/handler"
	channelreplica "github.com/WuKongIM/WuKongIM/pkg/channel/replica"
	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel/runtime"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	metadb "github.com/WuKongIM/WuKongIM/pkg/slot/meta"
)

type channelMetaSource interface {
	GetChannelRuntimeMeta(ctx context.Context, channelID string, channelType int64) (metadb.ChannelRuntimeMeta, error)
	ListChannelRuntimeMeta(ctx context.Context) ([]metadb.ChannelRuntimeMeta, error)
}

type hashSlotTableVersionSource interface {
	HashSlotTableVersion() uint64
}

type memoryGenerationStore struct {
	mu     sync.RWMutex
	values map[channel.ChannelKey]uint64
}

type channelReplicaFactory struct {
	db        *channelstore.Engine
	localNode channel.NodeID
	now       func() time.Time
}

type channelMetaCluster interface {
	ApplyMeta(meta channel.Meta) error
	RemoveLocal(key channel.ChannelKey) error
}

type channelMetaSync struct {
	source          channelMetaSource
	cluster         channelMetaCluster
	localNode       uint64
	refreshInterval time.Duration

	mu           sync.Mutex
	cancel       context.CancelFunc
	done         chan struct{}
	appliedLocal map[channel.ChannelKey]struct{}

	lastHashSlotTableVersion uint64
}

func newMemoryGenerationStore() *memoryGenerationStore {
	return &memoryGenerationStore{values: make(map[channel.ChannelKey]uint64)}
}

func (s *memoryGenerationStore) Load(channelKey channel.ChannelKey) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.values[channelKey], nil
}

func (s *memoryGenerationStore) Store(channelKey channel.ChannelKey, generation uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[channelKey] = generation
	return nil
}

func newChannelReplicaFactory(db *channelstore.Engine, localNode channel.NodeID, now func() time.Time) *channelReplicaFactory {
	return &channelReplicaFactory{db: db, localNode: localNode, now: now}
}

func (f *channelReplicaFactory) New(cfg channelruntime.ChannelConfig) (channelreplica.Replica, error) {
	store := f.db.ForChannel(cfg.ChannelKey, cfg.Meta.ID)
	return channelreplica.NewReplica(channelreplica.ReplicaConfig{
		LocalNode:         f.localNode,
		LogStore:          store,
		CheckpointStore:   channelCheckpointStore{store: store},
		ApplyFetchStore:   store,
		EpochHistoryStore: channelEpochHistoryStore{store: store},
		SnapshotApplier:   channelSnapshotApplier{store: store},
		Now:               f.now,
	})
}

type channelCheckpointStore struct{ store *channelstore.ChannelStore }

func (s channelCheckpointStore) Load() (channel.Checkpoint, error) { return s.store.LoadCheckpoint() }
func (s channelCheckpointStore) Store(cp channel.Checkpoint) error {
	return s.store.StoreCheckpoint(cp)
}

type channelEpochHistoryStore struct{ store *channelstore.ChannelStore }

func (s channelEpochHistoryStore) Load() ([]channel.EpochPoint, error) { return s.store.LoadHistory() }
func (s channelEpochHistoryStore) Append(point channel.EpochPoint) error {
	return s.store.AppendHistory(point)
}
func (s channelEpochHistoryStore) TruncateTo(leo uint64) error { return s.store.TruncateHistoryTo(leo) }

type channelSnapshotApplier struct{ store *channelstore.ChannelStore }

func (s channelSnapshotApplier) InstallSnapshot(_ context.Context, snap channel.Snapshot) error {
	return s.store.StoreSnapshotPayload(snap.Payload)
}

func (s *channelMetaSync) RefreshChannelMeta(ctx context.Context, id channel.ChannelID) (channel.Meta, error) {
	s.observeHashSlotTableVersion()
	meta, err := s.source.GetChannelRuntimeMeta(ctx, id.ID, int64(id.Type))
	if err != nil {
		return channel.Meta{}, err
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

	if err := s.syncOnce(ctx); err != nil {
		cancel()
		s.mu.Lock()
		if s.done == done {
			s.cancel = nil
			s.done = nil
		}
		s.mu.Unlock()
		return errors.Join(err, s.cleanupAppliedLocal())
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
	s.observeHashSlotTableVersion()
	metas, err := s.source.ListChannelRuntimeMeta(ctx)
	if err != nil {
		return err
	}
	currentLocal := make(map[channel.ChannelKey]struct{})
	for _, meta := range metas {
		if !containsUint64(meta.Replicas, s.localNode) {
			continue
		}
		applied, err := s.apply(meta)
		if err != nil {
			return err
		}
		currentLocal[applied.Key] = struct{}{}
	}
	for key := range s.snapshotAppliedLocal() {
		if _, ok := currentLocal[key]; ok {
			continue
		}
		if err := s.removeLocal(key); err != nil {
			return err
		}
	}
	s.setAppliedLocal(currentLocal)
	return nil
}

func (s *channelMetaSync) apply(meta metadb.ChannelRuntimeMeta) (channel.Meta, error) {
	if !containsUint64(meta.Replicas, s.localNode) {
		return channel.Meta{}, channel.ErrStaleMeta
	}
	rootMeta := projectChannelMeta(meta)
	if err := s.cluster.ApplyMeta(rootMeta); err != nil {
		return channel.Meta{}, err
	}
	s.mu.Lock()
	if s.appliedLocal == nil {
		s.appliedLocal = make(map[channel.ChannelKey]struct{})
	}
	s.appliedLocal[rootMeta.Key] = struct{}{}
	s.mu.Unlock()
	return rootMeta, nil
}

func (s *channelMetaSync) observeHashSlotTableVersion() {
	if s == nil || s.source == nil {
		return
	}
	versionSource, ok := s.source.(hashSlotTableVersionSource)
	if !ok {
		return
	}
	version := versionSource.HashSlotTableVersion()
	s.mu.Lock()
	if s.lastHashSlotTableVersion != 0 && version != s.lastHashSlotTableVersion {
		s.appliedLocal = nil
	}
	s.lastHashSlotTableVersion = version
	s.mu.Unlock()
}

func projectChannelMeta(meta metadb.ChannelRuntimeMeta) channel.Meta {
	id := channel.ChannelID{ID: meta.ChannelID, Type: uint8(meta.ChannelType)}
	var leaseUntil time.Time
	if meta.LeaseUntilMS > 0 {
		leaseUntil = time.UnixMilli(meta.LeaseUntilMS).UTC()
	}
	return channel.Meta{
		Key:         channelhandler.KeyFromChannelID(id),
		ID:          id,
		Epoch:       meta.ChannelEpoch,
		LeaderEpoch: meta.LeaderEpoch,
		Replicas:    projectNodeIDs(meta.Replicas),
		ISR:         projectNodeIDs(meta.ISR),
		Leader:      channel.NodeID(meta.Leader),
		MinISR:      int(meta.MinISR),
		LeaseUntil:  leaseUntil,
		Status:      channel.Status(meta.Status),
		Features: channel.Features{
			MessageSeqFormat: channel.MessageSeqFormat(meta.Features),
		},
	}
}

func projectNodeIDs(ids []uint64) []channel.NodeID {
	out := make([]channel.NodeID, 0, len(ids))
	for _, id := range ids {
		out = append(out, channel.NodeID(id))
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

func (s *channelMetaSync) removeLocal(key channel.ChannelKey) error {
	if s.cluster == nil {
		return nil
	}
	if err := s.cluster.RemoveLocal(key); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.appliedLocal, key)
	if len(s.appliedLocal) == 0 {
		s.appliedLocal = nil
	}
	return nil
}

func (s *channelMetaSync) cleanupAppliedLocal() error {
	var err error
	for key := range s.snapshotAppliedLocal() {
		err = errors.Join(err, s.removeLocal(key))
	}
	s.mu.Lock()
	s.appliedLocal = nil
	s.mu.Unlock()
	return err
}

func (s *channelMetaSync) snapshotAppliedLocal() map[channel.ChannelKey]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneAppliedLocalSet(s.appliedLocal)
}

func (s *channelMetaSync) setAppliedLocal(values map[channel.ChannelKey]struct{}) {
	s.mu.Lock()
	s.appliedLocal = cloneAppliedLocalSet(values)
	s.mu.Unlock()
}

func cloneAppliedLocalSet(values map[channel.ChannelKey]struct{}) map[channel.ChannelKey]struct{} {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[channel.ChannelKey]struct{}, len(values))
	for key := range values {
		cloned[key] = struct{}{}
	}
	return cloned
}
