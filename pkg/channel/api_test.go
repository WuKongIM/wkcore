package channel_test

import (
	"context"
	"errors"
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
			Build: buildTestRuntime,
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

func buildTestRuntime(cfg channel.RuntimeBuildConfig) (channel.Runtime, any, error) {
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

func buildTestHandler(cfg channel.HandlerBuildConfig) (channel.Service, error) {
	engine, ok := cfg.Store.(*channelstore.Engine)
	if !ok {
		return nil, errors.New("handler store type mismatch")
	}
	rt, ok := cfg.Runtime.(channelruntime.Runtime)
	if !ok {
		return nil, errors.New("handler runtime type mismatch")
	}
	messageIDs, ok := cfg.MessageIDs.(*channelTestMessageIDs)
	if !ok {
		return nil, errors.New("handler message id type mismatch")
	}
	return channelhandler.New(channelhandler.Config{
		Runtime:    rt,
		Store:      engine,
		MessageIDs: messageIDs,
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
