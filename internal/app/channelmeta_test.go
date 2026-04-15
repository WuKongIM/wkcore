package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	channelhandler "github.com/WuKongIM/WuKongIM/pkg/channel/handler"
	metadb "github.com/WuKongIM/WuKongIM/pkg/slot/meta"
	"github.com/stretchr/testify/require"
)

func TestChannelMetaSyncRefreshProjectsLeaderEpochLeaseAndApply(t *testing.T) {
	leaseUntil := time.UnixMilli(1_700_000_123_000).UTC()
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    "u1",
		ChannelType:  1,
		ChannelEpoch: 5,
		LeaderEpoch:  7,
		Replicas:     []uint64{2, 3},
		ISR:          []uint64{2, 3},
		Leader:       2,
		MinISR:       2,
		Status:       uint8(channel.StatusActive),
		Features:     uint64(channel.MessageSeqFormatU64),
		LeaseUntilMS: leaseUntil.UnixMilli(),
	}

	source := &fakeChannelMetaSource{
		get: map[channel.ChannelID]metadb.ChannelRuntimeMeta{
			{ID: "u1", Type: 1}: meta,
		},
	}
	cluster := &fakeChannelMetaCluster{}
	syncer := &channelMetaSync{
		source:    source,
		cluster:   cluster,
		localNode: 2,
	}

	got, err := syncer.RefreshChannelMeta(context.Background(), channel.ChannelID{ID: "u1", Type: 1})
	require.NoError(t, err)
	require.Equal(t, channel.Meta{
		Key:         channelhandler.KeyFromChannelID(channel.ChannelID{ID: "u1", Type: 1}),
		ID:          channel.ChannelID{ID: "u1", Type: 1},
		Epoch:       5,
		LeaderEpoch: 7,
		Replicas:    []channel.NodeID{2, 3},
		ISR:         []channel.NodeID{2, 3},
		Leader:      2,
		MinISR:      2,
		LeaseUntil:  leaseUntil,
		Status:      channel.StatusActive,
		Features: channel.Features{
			MessageSeqFormat: channel.MessageSeqFormatU64,
		},
	}, got)
	require.Equal(t, []channel.Meta{got}, cluster.applied)
	require.Equal(t, map[channel.ChannelKey]struct{}{got.Key: {}}, cloneAppliedLocalSet(syncer.appliedLocal))
}

func TestProjectChannelMetaKeepsZeroLeaseUnset(t *testing.T) {
	meta := projectChannelMeta(metadb.ChannelRuntimeMeta{
		ChannelID:   "u0",
		ChannelType: 1,
		LeaderEpoch: 3,
		Leader:      2,
		Replicas:    []uint64{2, 3},
		ISR:         []uint64{2, 3},
		MinISR:      2,
	})

	require.True(t, meta.LeaseUntil.IsZero())
	require.Equal(t, channelhandler.KeyFromChannelID(channel.ChannelID{ID: "u0", Type: 1}), meta.Key)
}

func TestChannelMetaSyncSyncOnceAppliesOnlyLocalReplicaMetas(t *testing.T) {
	source := &fakeChannelMetaSource{
		list: []metadb.ChannelRuntimeMeta{
			{
				ChannelID:    "local",
				ChannelType:  1,
				ChannelEpoch: 1,
				LeaderEpoch:  2,
				Replicas:     []uint64{2, 3},
				ISR:          []uint64{2, 3},
				Leader:       2,
				MinISR:       1,
				Status:       uint8(channel.StatusActive),
				Features:     uint64(channel.MessageSeqFormatLegacyU32),
			},
			{
				ChannelID:    "remote",
				ChannelType:  1,
				ChannelEpoch: 3,
				LeaderEpoch:  4,
				Replicas:     []uint64{7, 8},
				ISR:          []uint64{7, 8},
				Leader:       7,
				MinISR:       1,
				Status:       uint8(channel.StatusActive),
				Features:     uint64(channel.MessageSeqFormatLegacyU32),
			},
		},
	}
	cluster := &fakeChannelMetaCluster{}
	syncer := &channelMetaSync{
		source:    source,
		cluster:   cluster,
		localNode: 2,
	}

	require.NoError(t, syncer.syncOnce(context.Background()))
	require.Len(t, cluster.applied, 1)
	require.Equal(t, channel.ChannelID{ID: "local", Type: 1}, cluster.applied[0].ID)
	require.Equal(t, map[channel.ChannelKey]struct{}{
		cluster.applied[0].Key: {},
	}, cloneAppliedLocalSet(syncer.appliedLocal))
}

func TestChannelMetaSyncRefreshRejectsNonLocalReplicaMeta(t *testing.T) {
	source := &fakeChannelMetaSource{
		get: map[channel.ChannelID]metadb.ChannelRuntimeMeta{
			{ID: "remote", Type: 1}: {
				ChannelID:    "remote",
				ChannelType:  1,
				ChannelEpoch: 2,
				LeaderEpoch:  3,
				Replicas:     []uint64{7, 8},
				ISR:          []uint64{7, 8},
				Leader:       7,
				MinISR:       1,
				Status:       uint8(channel.StatusActive),
				Features:     uint64(channel.MessageSeqFormatLegacyU32),
			},
		},
	}
	cluster := &fakeChannelMetaCluster{}
	syncer := &channelMetaSync{
		source:    source,
		cluster:   cluster,
		localNode: 2,
	}

	_, err := syncer.RefreshChannelMeta(context.Background(), channel.ChannelID{ID: "remote", Type: 1})
	require.ErrorIs(t, err, channel.ErrStaleMeta)
	require.Empty(t, cluster.applied)
	require.Nil(t, cloneAppliedLocalSet(syncer.appliedLocal))
}

func TestChannelMetaSyncRefreshClearsAppliedLocalOnHashSlotTableVersionChange(t *testing.T) {
	staleKey := channelhandler.KeyFromChannelID(channel.ChannelID{ID: "stale", Type: 1})
	freshID := channel.ChannelID{ID: "fresh", Type: 1}
	source := &fakeChannelMetaSource{
		version: 2,
		get: map[channel.ChannelID]metadb.ChannelRuntimeMeta{
			freshID: {
				ChannelID:    freshID.ID,
				ChannelType:  int64(freshID.Type),
				ChannelEpoch: 3,
				LeaderEpoch:  4,
				Replicas:     []uint64{2},
				ISR:          []uint64{2},
				Leader:       2,
				MinISR:       1,
				Status:       uint8(channel.StatusActive),
			},
		},
	}
	cluster := &fakeChannelMetaCluster{}
	syncer := &channelMetaSync{
		source:                   source,
		cluster:                  cluster,
		localNode:                2,
		lastHashSlotTableVersion: 1,
		appliedLocal: map[channel.ChannelKey]struct{}{
			staleKey: {},
		},
	}

	got, err := syncer.RefreshChannelMeta(context.Background(), freshID)
	require.NoError(t, err)
	require.Equal(t, map[channel.ChannelKey]struct{}{got.Key: {}}, cloneAppliedLocalSet(syncer.appliedLocal))
}

func TestChannelMetaSyncSyncOnceClearsAppliedLocalOnHashSlotTableVersionChange(t *testing.T) {
	staleKey := channelhandler.KeyFromChannelID(channel.ChannelID{ID: "stale", Type: 1})
	freshID := channel.ChannelID{ID: "fresh", Type: 1}
	source := &fakeChannelMetaSource{
		version: 2,
		list: []metadb.ChannelRuntimeMeta{
			{
				ChannelID:    freshID.ID,
				ChannelType:  int64(freshID.Type),
				ChannelEpoch: 3,
				LeaderEpoch:  4,
				Replicas:     []uint64{2},
				ISR:          []uint64{2},
				Leader:       2,
				MinISR:       1,
				Status:       uint8(channel.StatusActive),
			},
		},
	}
	cluster := &fakeChannelMetaCluster{}
	syncer := &channelMetaSync{
		source:                   source,
		cluster:                  cluster,
		localNode:                2,
		lastHashSlotTableVersion: 1,
		appliedLocal: map[channel.ChannelKey]struct{}{
			staleKey: {},
		},
	}

	require.NoError(t, syncer.syncOnce(context.Background()))
	require.Equal(t, []channel.Meta{projectChannelMeta(source.list[0])}, cluster.applied)
	require.Empty(t, cluster.removed)
	require.Equal(t, map[channel.ChannelKey]struct{}{
		channelhandler.KeyFromChannelID(freshID): {},
	}, cloneAppliedLocalSet(syncer.appliedLocal))
}

func TestChannelMetaSyncSyncOnceRemovesChannelsNoLongerAssignedLocally(t *testing.T) {
	key := channelhandler.KeyFromChannelID(channel.ChannelID{ID: "local", Type: 1})
	source := &fakeChannelMetaSource{
		list: []metadb.ChannelRuntimeMeta{
			{
				ChannelID:    "other",
				ChannelType:  1,
				ChannelEpoch: 5,
				LeaderEpoch:  6,
				Replicas:     []uint64{9},
				ISR:          []uint64{9},
				Leader:       9,
				MinISR:       1,
				Status:       uint8(channel.StatusActive),
				Features:     uint64(channel.MessageSeqFormatLegacyU32),
			},
		},
	}
	cluster := &fakeChannelMetaCluster{}
	syncer := &channelMetaSync{
		source:    source,
		cluster:   cluster,
		localNode: 2,
		appliedLocal: map[channel.ChannelKey]struct{}{
			key: {},
		},
	}

	require.NoError(t, syncer.syncOnce(context.Background()))
	require.Equal(t, []channel.ChannelKey{key}, cluster.removed)
	require.Equal(t, map[channel.ChannelKey]struct{}{}, nonNilAppliedLocal(syncer.appliedLocal))
}

func TestChannelMetaSyncStopRemovesAppliedLocalChannels(t *testing.T) {
	key := channelhandler.KeyFromChannelID(channel.ChannelID{ID: "local", Type: 1})
	done := make(chan struct{})
	cluster := &fakeChannelMetaCluster{}
	syncer := &channelMetaSync{
		cluster: cluster,
		cancel: func() {
			close(done)
		},
		done: done,
		appliedLocal: map[channel.ChannelKey]struct{}{
			key: {},
		},
	}

	require.NoError(t, syncer.Stop())
	require.Equal(t, []channel.ChannelKey{key}, cluster.removed)
	require.Nil(t, cloneAppliedLocalSet(syncer.appliedLocal))
}

func TestMemoryGenerationStoreConcurrentAccess(t *testing.T) {
	store := newMemoryGenerationStore()
	keys := []channel.ChannelKey{
		"a",
		"b",
		"c",
		"d",
	}

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := keys[(worker+i)%len(keys)]
				require.NoError(t, store.Store(key, uint64(worker+i)))
				_, err := store.Load(key)
				require.NoError(t, err)
			}
		}(worker)
	}
	wg.Wait()
}

func TestChannelMetaSyncConcurrentRefreshAndStop(t *testing.T) {
	id := channel.ChannelID{ID: "race", Type: 1}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    id.ID,
		ChannelType:  int64(id.Type),
		ChannelEpoch: 5,
		LeaderEpoch:  7,
		Replicas:     []uint64{2, 3},
		ISR:          []uint64{2, 3},
		Leader:       2,
		MinISR:       2,
		Status:       uint8(channel.StatusActive),
		Features:     uint64(channel.MessageSeqFormatU64),
		LeaseUntilMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	source := &fakeChannelMetaSource{
		get: map[channel.ChannelID]metadb.ChannelRuntimeMeta{
			id: meta,
		},
		list: []metadb.ChannelRuntimeMeta{meta},
	}
	cluster := &fakeChannelMetaCluster{}
	syncer := &channelMetaSync{
		source:          source,
		cluster:         cluster,
		localNode:       2,
		refreshInterval: time.Millisecond,
	}

	require.NoError(t, syncer.Start())

	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_, _ = syncer.RefreshChannelMeta(context.Background(), id)
			}
		}()
	}

	require.NoError(t, syncer.Stop())
	wg.Wait()
}

type fakeChannelMetaSource struct {
	get     map[channel.ChannelID]metadb.ChannelRuntimeMeta
	list    []metadb.ChannelRuntimeMeta
	version uint64
}

func (f *fakeChannelMetaSource) GetChannelRuntimeMeta(_ context.Context, channelID string, channelType int64) (metadb.ChannelRuntimeMeta, error) {
	return f.get[channel.ChannelID{ID: channelID, Type: uint8(channelType)}], nil
}

func (f *fakeChannelMetaSource) ListChannelRuntimeMeta(context.Context) ([]metadb.ChannelRuntimeMeta, error) {
	return append([]metadb.ChannelRuntimeMeta(nil), f.list...), nil
}

func (f *fakeChannelMetaSource) HashSlotTableVersion() uint64 {
	return f.version
}

type fakeChannelMetaCluster struct {
	mu        sync.Mutex
	applied   []channel.Meta
	removed   []channel.ChannelKey
	applyErr  error
	removeErr error
}

func (f *fakeChannelMetaCluster) ApplyMeta(meta channel.Meta) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, meta)
	return f.applyErr
}

func (f *fakeChannelMetaCluster) RemoveLocal(key channel.ChannelKey) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, key)
	return f.removeErr
}

func nonNilAppliedLocal(values map[channel.ChannelKey]struct{}) map[channel.ChannelKey]struct{} {
	if len(values) == 0 {
		return map[channel.ChannelKey]struct{}{}
	}
	return cloneAppliedLocalSet(values)
}
