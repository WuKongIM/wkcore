package channel_test

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	channelhandler "github.com/WuKongIM/WuKongIM/pkg/channel/handler"
	channelreplica "github.com/WuKongIM/WuKongIM/pkg/channel/replica"
	channelruntime "github.com/WuKongIM/WuKongIM/pkg/channel/runtime"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	channeltransport "github.com/WuKongIM/WuKongIM/pkg/channel/transport"
	wktransport "github.com/WuKongIM/WuKongIM/pkg/transport"
)

func TestNewBuildsClusterWithRuntimeHandlerAndTransport(t *testing.T) {
	engine, err := channelstore.Open(t.TempDir())
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	defer func() {
		if err := engine.Close(); err != nil {
			t.Fatalf("engine.Close() error = %v", err)
		}
	}()

	client := wktransport.NewClient(wktransport.NewPool(channelTestDiscovery{addrs: map[uint64]string{}}, 1, time.Second))
	defer client.Stop()

	var runtimeToClose interface{ Close() error }

	got, err := channel.New(channel.Config{
		LocalNode:       1,
		Store:           engine,
		GenerationStore: &channelTestGenerationStore{},
		MessageIDs:      &channelTestMessageIDs{},
		Transport: channel.TransportConfig{
			Client: client,
			RPCMux: wktransport.NewRPCMux(),
			Build:  buildTestTransport,
		},
		Runtime: channel.RuntimeConfig{
			Limits: channel.RuntimeLimits{
				MaxFetchInflightPeer: 1,
				MaxSnapshotInflight:  1,
			},
			Tombstones: channel.RuntimeTombstones{
				TombstoneTTL:    time.Minute,
				CleanupInterval: time.Minute,
			},
			Build: func(cfg channel.RuntimeBuildConfig) (channel.Runtime, channel.HandlerRuntime, error) {
				control, runtimeValue, err := buildTestRuntime(cfg)
				if err != nil {
					return nil, nil, err
				}
				closer, ok := runtimeValue.(interface{ Close() error })
				if !ok {
					t.Fatalf("runtime build value %T does not implement Close", runtimeValue)
				}
				runtimeToClose = closer
				return control, runtimeValue, nil
			},
		},
		Handler: channel.HandlerConfig{
			Build: buildTestHandler,
		},
		Now: func() time.Time {
			return time.Unix(1700000000, 0)
		},
	})
	if err != nil {
		t.Fatalf("channel.New() error = %v", err)
	}
	if got == nil {
		t.Fatal("channel.New() returned nil cluster")
	}
	if runtimeToClose == nil {
		t.Fatal("runtime builder did not capture runtime closer")
	}
	defer func() {
		if err := runtimeToClose.Close(); err != nil {
			t.Fatalf("runtime.Close() error = %v", err)
		}
	}()

	id := channel.ChannelID{ID: "room-1", Type: 1}
	meta := channel.Meta{
		ID:          id,
		Epoch:       1,
		LeaderEpoch: 1,
		Leader:      1,
		Replicas:    []channel.NodeID{1},
		ISR:         []channel.NodeID{1},
		MinISR:      1,
		Status:      channel.StatusActive,
		Features:    channel.Features{MessageSeqFormat: channel.MessageSeqFormatU64},
		LeaseUntil:  time.Unix(1700000060, 0),
	}
	if err := got.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}

	appendResult, err := got.Append(context.Background(), channel.AppendRequest{
		ChannelID:             id,
		SupportsMessageSeqU64: true,
		Message: channel.Message{
			FromUID: "alice",
			Payload: []byte("hello"),
		},
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if appendResult.MessageSeq != 1 {
		t.Fatalf("Append().MessageSeq = %d, want 1", appendResult.MessageSeq)
	}
	if appendResult.Message.MessageID == 0 {
		t.Fatal("Append().Message.MessageID = 0, want non-zero")
	}

	fetchResult, err := got.Fetch(context.Background(), channel.FetchRequest{
		ChannelID: id,
		Limit:     10,
		MaxBytes:  1024,
	})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(fetchResult.Messages) != 1 {
		t.Fatalf("Fetch().Messages len = %d, want 1", len(fetchResult.Messages))
	}
	if string(fetchResult.Messages[0].Payload) != "hello" {
		t.Fatalf("Fetch().Messages[0].Payload = %q, want %q", fetchResult.Messages[0].Payload, "hello")
	}
	if fetchResult.CommittedSeq != 1 {
		t.Fatalf("Fetch().CommittedSeq = %d, want 1", fetchResult.CommittedSeq)
	}

	status, err := got.Status(id)
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	if status.Key != channel.ChannelKey("channel/1/cm9vbS0x") {
		t.Fatalf("Status().Key = %q, want %q", status.Key, channel.ChannelKey("channel/1/cm9vbS0x"))
	}
	if status.CommittedSeq != 1 || status.HW != 1 {
		t.Fatalf("Status() commit state = %+v, want committed seq/hw 1", status)
	}
}

func TestApplyMetaRollsBackHandlerStateWhenRuntimeMutationFails(t *testing.T) {
	meta := channel.Meta{
		ID:          channel.ChannelID{ID: "rollback", Type: 1},
		Epoch:       2,
		LeaderEpoch: 3,
		Leader:      1,
		Replicas:    []channel.NodeID{1},
		ISR:         []channel.NodeID{1},
		MinISR:      1,
		Status:      channel.StatusActive,
		Features:    channel.Features{MessageSeqFormat: channel.MessageSeqFormatU64},
	}

	t.Run("create rollback removes speculative handler meta", func(t *testing.T) {
		service := newStubMetaService()
		cluster := mustBuildStubCluster(t, service, &stubRuntimeControl{
			upsertErr: errors.New("upsert failed"),
		})

		err := cluster.ApplyMeta(meta)
		if err == nil {
			t.Fatal("expected ApplyMeta() to fail")
		}

		key := encodedTestChannelKey(meta.ID)
		if _, ok := service.MetaSnapshot(key); ok {
			t.Fatalf("handler meta for %q remained after runtime upsert failure", key)
		}
	})

	t.Run("delete rollback restores previous handler meta", func(t *testing.T) {
		service := newStubMetaService()
		key := encodedTestChannelKey(meta.ID)
		service.ForceSetMeta(key, meta)
		cluster := mustBuildStubCluster(t, service, &stubRuntimeControl{
			removeErr: errors.New("remove failed"),
		})

		deleted := meta
		deleted.Status = channel.StatusDeleted
		err := cluster.ApplyMeta(deleted)
		if err == nil {
			t.Fatal("expected ApplyMeta() to fail")
		}

		got, ok := service.MetaSnapshot(key)
		if !ok {
			t.Fatalf("handler meta for %q missing after rollback", key)
		}
		if got.Status != channel.StatusActive {
			t.Fatalf("handler meta status = %d, want %d", got.Status, channel.StatusActive)
		}
		if got.Epoch != meta.Epoch || got.LeaderEpoch != meta.LeaderEpoch {
			t.Fatalf("handler meta after rollback = %+v, want %+v", got, meta)
		}
	})
}

func TestNewClosesBuiltRuntimeWhenHandlerBuildFails(t *testing.T) {
	runtimeControl := &stubRuntimeControl{}
	runtimeValue := &stubClosableRuntimeValue{}

	_, err := channel.New(channel.Config{
		LocalNode:       1,
		Store:           struct{}{},
		GenerationStore: struct{}{},
		MessageIDs:      &channelTestMessageIDs{},
		Transport: channel.TransportConfig{
			Build: func(channel.TransportBuildConfig) (any, error) {
				return struct{}{}, nil
			},
		},
		Runtime: channel.RuntimeConfig{
			Build: func(channel.RuntimeBuildConfig) (channel.Runtime, channel.HandlerRuntime, error) {
				return runtimeControl, runtimeValue, nil
			},
		},
		Handler: channel.HandlerConfig{
			Build: func(channel.HandlerBuildConfig) (channel.MetaRollbackService, error) {
				return nil, errors.New("handler build failed")
			},
		},
	})
	if err == nil {
		t.Fatal("expected channel.New() to fail")
	}
	if runtimeControl.closeCalls != 0 {
		t.Fatalf("runtime control close calls = %d, want 0 when only runtime value owns cleanup", runtimeControl.closeCalls)
	}
	if runtimeValue.closeCalls != 1 {
		t.Fatalf("runtime value close calls = %d, want 1", runtimeValue.closeCalls)
	}
}

func buildTestTransport(cfg channel.TransportBuildConfig) (any, error) {
	client, ok := cfg.Client.(*wktransport.Client)
	if !ok {
		return nil, errors.New("transport client type mismatch")
	}
	mux, ok := cfg.RPCMux.(*wktransport.RPCMux)
	if !ok {
		return nil, errors.New("transport mux type mismatch")
	}
	return channeltransport.New(channeltransport.Options{
		LocalNode:          cfg.LocalNode,
		Client:             client,
		RPCMux:             mux,
		RPCTimeout:         cfg.RPCTimeout,
		MaxPendingFetchRPC: cfg.MaxPendingFetchRPC,
	})
}

func buildTestRuntime(cfg channel.RuntimeBuildConfig) (channel.Runtime, channel.HandlerRuntime, error) {
	engine, ok := cfg.Store.(*channelstore.Engine)
	if !ok {
		return nil, nil, errors.New("runtime store type mismatch")
	}
	generationStore, ok := cfg.GenerationStore.(*channelTestGenerationStore)
	if !ok {
		return nil, nil, errors.New("runtime generation store type mismatch")
	}
	transportValue, ok := cfg.Transport.(*channeltransport.Transport)
	if !ok {
		return nil, nil, errors.New("runtime transport type mismatch")
	}

	rt, err := channelruntime.New(channelruntime.Config{
		LocalNode:                        cfg.LocalNode,
		ReplicaFactory:                   channelTestReplicaFactory{localNode: cfg.LocalNode, store: engine, now: cfg.Now},
		GenerationStore:                  generationStore,
		Transport:                        transportValue,
		PeerSessions:                     transportValue,
		AutoRunScheduler:                 cfg.AutoRunScheduler,
		FollowerReplicationRetryInterval: cfg.FollowerReplicationRetryInterval,
		Limits: channelruntime.Limits{
			MaxChannels:               cfg.Limits.MaxChannels,
			MaxFetchInflightPeer:      cfg.Limits.MaxFetchInflightPeer,
			MaxSnapshotInflight:       cfg.Limits.MaxSnapshotInflight,
			MaxRecoveryBytesPerSecond: cfg.Limits.MaxRecoveryBytesPerSecond,
		},
		Tombstones: channelruntime.TombstonePolicy{
			TombstoneTTL:    cfg.Tombstones.TombstoneTTL,
			CleanupInterval: cfg.Tombstones.CleanupInterval,
		},
		Now: cfg.Now,
	})
	if err != nil {
		return nil, nil, err
	}
	transportValue.BindFetchService(rt)
	return channelRuntimeControl{runtime: rt}, rt, nil
}

func buildTestHandler(cfg channel.HandlerBuildConfig) (channel.MetaRollbackService, error) {
	engine, ok := cfg.Store.(*channelstore.Engine)
	if !ok {
		return nil, errors.New("handler store type mismatch")
	}
	rt, ok := cfg.Runtime.(channel.HandlerRuntime)
	if !ok {
		return nil, errors.New("handler runtime type mismatch")
	}
	return channelhandler.New(channelhandler.Config{
		Runtime:    rt,
		Store:      engine,
		MessageIDs: cfg.MessageIDs,
	})
}

type channelRuntimeControl struct {
	runtime channelruntime.Runtime
}

func (c channelRuntimeControl) UpsertMeta(meta channel.Meta) error {
	if err := c.runtime.EnsureChannel(meta); err != nil {
		if errors.Is(err, channelruntime.ErrChannelExists) {
			return c.runtime.ApplyMeta(meta)
		}
		return err
	}
	return nil
}

func (c channelRuntimeControl) RemoveChannel(key channel.ChannelKey) error {
	if err := c.runtime.RemoveChannel(key); err != nil && !errors.Is(err, channel.ErrChannelNotFound) {
		return err
	}
	return nil
}

type channelTestReplicaFactory struct {
	localNode channel.NodeID
	store     *channelstore.Engine
	now       func() time.Time
}

func (f channelTestReplicaFactory) New(cfg channelruntime.ChannelConfig) (channelreplica.Replica, error) {
	store := f.store.ForChannel(cfg.ChannelKey, cfg.Meta.ID)
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

type channelTestGenerationStore struct{}

func (channelTestGenerationStore) Load(channel.ChannelKey) (uint64, error) { return 0, nil }
func (channelTestGenerationStore) Store(channel.ChannelKey, uint64) error  { return nil }

type channelTestMessageIDs struct{ next uint64 }

func (g *channelTestMessageIDs) Next() uint64 {
	g.next++
	return g.next
}

type channelTestDiscovery struct {
	addrs map[uint64]string
}

func (d channelTestDiscovery) Resolve(nodeID uint64) (string, error) {
	addr, ok := d.addrs[nodeID]
	if !ok {
		return "", wktransport.ErrNodeNotFound
	}
	return addr, nil
}

func mustBuildStubCluster(t *testing.T, service channel.MetaRollbackService, runtimeControl channel.Runtime) channel.Cluster {
	t.Helper()

	got, err := channel.New(channel.Config{
		LocalNode:       1,
		Store:           struct{}{},
		GenerationStore: struct{}{},
		MessageIDs:      &channelTestMessageIDs{},
		Transport: channel.TransportConfig{
			Build: func(channel.TransportBuildConfig) (any, error) {
				return struct{}{}, nil
			},
		},
		Runtime: channel.RuntimeConfig{
			Build: func(channel.RuntimeBuildConfig) (channel.Runtime, channel.HandlerRuntime, error) {
				return runtimeControl, stubHandlerRuntime{}, nil
			},
		},
		Handler: channel.HandlerConfig{
			Build: func(channel.HandlerBuildConfig) (channel.MetaRollbackService, error) {
				return service, nil
			},
		},
		Now: func() time.Time {
			return time.Unix(1700000000, 0)
		},
	})
	if err != nil {
		t.Fatalf("channel.New() error = %v", err)
	}
	return got
}

func encodedTestChannelKey(id channel.ChannelID) channel.ChannelKey {
	encodedID := base64.RawURLEncoding.EncodeToString([]byte(id.ID))
	return channel.ChannelKey("channel/" + strconv.FormatUint(uint64(id.Type), 10) + "/" + encodedID)
}

type stubRuntimeControl struct {
	upsertErr  error
	removeErr  error
	closeCalls int
}

func (r *stubRuntimeControl) UpsertMeta(channel.Meta) error { return r.upsertErr }
func (r *stubRuntimeControl) RemoveChannel(channel.ChannelKey) error {
	return r.removeErr
}
func (r *stubRuntimeControl) Close() error {
	r.closeCalls++
	return nil
}

type stubClosableRuntimeValue struct {
	closeCalls int
}

func (r *stubClosableRuntimeValue) Close() error {
	r.closeCalls++
	return nil
}

func (*stubClosableRuntimeValue) Channel(channel.ChannelKey) (channel.HandlerChannel, bool) {
	return nil, false
}

type stubHandlerRuntime struct{}

func (stubHandlerRuntime) Channel(channel.ChannelKey) (channel.HandlerChannel, bool) {
	return nil, false
}

type stubMetaService struct {
	metas map[channel.ChannelKey]channel.Meta
}

func newStubMetaService() *stubMetaService {
	return &stubMetaService{metas: make(map[channel.ChannelKey]channel.Meta)}
}

func (s *stubMetaService) ApplyMeta(meta channel.Meta) error {
	s.metas[meta.Key] = meta
	return nil
}

func (s *stubMetaService) Append(context.Context, channel.AppendRequest) (channel.AppendResult, error) {
	return channel.AppendResult{}, errors.New("not implemented")
}

func (s *stubMetaService) Fetch(context.Context, channel.FetchRequest) (channel.FetchResult, error) {
	return channel.FetchResult{}, errors.New("not implemented")
}

func (s *stubMetaService) Status(channel.ChannelID) (channel.ChannelRuntimeStatus, error) {
	return channel.ChannelRuntimeStatus{}, errors.New("not implemented")
}

func (s *stubMetaService) MetaSnapshot(key channel.ChannelKey) (channel.Meta, bool) {
	meta, ok := s.metas[key]
	return meta, ok
}

func (s *stubMetaService) RestoreMeta(key channel.ChannelKey, meta channel.Meta, ok bool) {
	if !ok {
		delete(s.metas, key)
		return
	}
	s.metas[key] = meta
}

func (s *stubMetaService) ForceSetMeta(key channel.ChannelKey, meta channel.Meta) {
	s.metas[key] = meta
}
