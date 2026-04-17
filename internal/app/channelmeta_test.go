package app

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	channelhandler "github.com/WuKongIM/WuKongIM/pkg/channel/handler"
	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
	metadb "github.com/WuKongIM/WuKongIM/pkg/slot/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
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

func TestChannelMetaSyncRefreshReturnsExistingAuthoritativeMetaWithoutBootstrap(t *testing.T) {
	id := channel.ChannelID{ID: "g1", Type: 2}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    id.ID,
		ChannelType:  int64(id.Type),
		ChannelEpoch: 1,
		LeaderEpoch:  3,
		Replicas:     []uint64{2, 4},
		ISR:          []uint64{2, 4},
		Leader:       2,
		MinISR:       2,
		Status:       uint8(channel.StatusActive),
	}
	source := &fakeChannelMetaSource{
		get: map[channel.ChannelID]metadb.ChannelRuntimeMeta{id: meta},
	}
	bootstrapStore := &fakeBootstrapStore{getErr: metadb.ErrNotFound}
	bootstrapCluster := &fakeBootstrapCluster{slotID: 9, peers: []uint64{2, 4}, leader: 2}
	cluster := &fakeChannelMetaCluster{}
	syncer := &channelMetaSync{
		source:    source,
		cluster:   cluster,
		bootstrap: newChannelMetaBootstrapper(bootstrapCluster, bootstrapStore, 2, time.Now, wklog.NewNop()),
		localNode: 2,
	}

	got, err := syncer.RefreshChannelMeta(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, uint64(1), got.Epoch)
	require.Equal(t, []channel.Meta{got}, cluster.applied)
	require.Empty(t, bootstrapStore.upserts)
	require.Zero(t, bootstrapCluster.leaderCalls)
}

func TestChannelMetaSyncRefreshBootstrapsOnAuthoritativeMiss(t *testing.T) {
	id := channel.ChannelID{ID: "g1", Type: 2}
	authoritative := metadb.ChannelRuntimeMeta{
		ChannelID:    id.ID,
		ChannelType:  int64(id.Type),
		ChannelEpoch: 1,
		LeaderEpoch:  4,
		Replicas:     []uint64{2, 7},
		ISR:          []uint64{2, 7},
		Leader:       2,
		MinISR:       2,
		Status:       uint8(channel.StatusActive),
	}
	source := &fakeChannelMetaSource{
		getResults: []fakeChannelMetaGetResult{
			{err: metadb.ErrNotFound},
			{err: metadb.ErrNotFound},
			{meta: authoritative},
			{meta: authoritative},
		},
	}
	bootstrapCluster := &fakeBootstrapCluster{slotID: 3, peers: []uint64{2, 7}, leader: 2}
	cluster := &fakeChannelMetaCluster{}
	syncer := &channelMetaSync{
		source:    source,
		cluster:   cluster,
		bootstrap: newChannelMetaBootstrapper(bootstrapCluster, source, 2, time.Now, wklog.NewNop()),
		localNode: 2,
	}

	got, err := syncer.RefreshChannelMeta(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, uint64(1), got.Epoch)
	require.Equal(t, []channel.Meta{got}, cluster.applied)
	require.Len(t, source.upserts, 1)
	require.Equal(t, authoritative, source.lastGetMeta)
}

func TestChannelMetaSyncRefreshAppliesAuthoritativeRereadInsteadOfBootstrapCandidate(t *testing.T) {
	id := channel.ChannelID{ID: "g1", Type: 2}
	authoritative := metadb.ChannelRuntimeMeta{
		ChannelID:    id.ID,
		ChannelType:  int64(id.Type),
		ChannelEpoch: 9,
		LeaderEpoch:  11,
		Replicas:     []uint64{2, 8, 9},
		ISR:          []uint64{2, 8},
		Leader:       8,
		MinISR:       2,
		Status:       uint8(channel.StatusActive),
		Features:     uint64(channel.MessageSeqFormatU64),
	}
	source := &fakeChannelMetaSource{
		getResults: []fakeChannelMetaGetResult{
			{err: metadb.ErrNotFound},
			{err: metadb.ErrNotFound},
			{meta: authoritative},
			{meta: authoritative},
		},
	}
	bootstrapCluster := &fakeBootstrapCluster{slotID: 5, peers: []uint64{2, 7, 8}, leader: 2}
	cluster := &fakeChannelMetaCluster{}
	syncer := &channelMetaSync{
		source:    source,
		cluster:   cluster,
		bootstrap: newChannelMetaBootstrapper(bootstrapCluster, source, 2, time.Now, wklog.NewNop()),
		localNode: 2,
	}

	got, err := syncer.RefreshChannelMeta(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, authoritative.ChannelEpoch, got.Epoch)
	require.Equal(t, authoritative.LeaderEpoch, got.LeaderEpoch)
	require.Equal(t, channel.NodeID(authoritative.Leader), got.Leader)
	require.Equal(t, projectChannelMeta(authoritative), got)
	require.Len(t, source.upserts, 1)
	require.NotEqual(t, authoritative.Leader, source.upserts[0].Leader)
	require.NotEqual(t, authoritative.ChannelEpoch, source.upserts[0].ChannelEpoch)
	require.Equal(t, []channel.Meta{projectChannelMeta(authoritative)}, cluster.applied)
}

func TestChannelMetaSyncRefreshDoesNotApplyPartialMetadataWhenBootstrapFails(t *testing.T) {
	id := channel.ChannelID{ID: "g1", Type: 2}
	bootstrapErr := errors.New("write failed")
	source := &fakeChannelMetaSource{
		getResults: []fakeChannelMetaGetResult{
			{err: metadb.ErrNotFound},
			{err: metadb.ErrNotFound},
		},
		upsertErr: bootstrapErr,
	}
	bootstrapCluster := &fakeBootstrapCluster{slotID: 7, peers: []uint64{2, 4}, leader: 2}
	cluster := &fakeChannelMetaCluster{}
	syncer := &channelMetaSync{
		source:    source,
		cluster:   cluster,
		bootstrap: newChannelMetaBootstrapper(bootstrapCluster, source, 2, time.Now, wklog.NewNop()),
		localNode: 2,
	}

	_, err := syncer.RefreshChannelMeta(context.Background(), id)
	require.ErrorContains(t, err, "upsert runtime metadata")
	require.ErrorIs(t, err, bootstrapErr)
	require.Empty(t, cluster.applied)
	require.Nil(t, cloneAppliedLocalSet(syncer.appliedLocal))
	require.Len(t, source.upserts, 1)
}

func TestChannelMetaSyncRefreshPreservesRetryableBootstrapErrorsWithoutApplyingLocalState(t *testing.T) {
	testCases := []struct {
		name string
		err  error
	}{
		{name: "no leader", err: raftcluster.ErrNoLeader},
		{name: "slot not found", err: raftcluster.ErrSlotNotFound},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			id := channel.ChannelID{ID: "g1", Type: 2}
			source := &fakeChannelMetaSource{
				getResults: []fakeChannelMetaGetResult{
					{err: metadb.ErrNotFound},
					{err: metadb.ErrNotFound},
				},
			}
			bootstrapCluster := &fakeBootstrapCluster{
				slotID:    8,
				peers:     []uint64{2, 4},
				leaderErr: tt.err,
			}
			cluster := &fakeChannelMetaCluster{}
			syncer := &channelMetaSync{
				source:    source,
				cluster:   cluster,
				bootstrap: newChannelMetaBootstrapper(bootstrapCluster, source, 2, time.Now, wklog.NewNop()),
				localNode: 2,
			}

			_, err := syncer.RefreshChannelMeta(context.Background(), id)
			require.ErrorIs(t, err, tt.err)
			require.Empty(t, cluster.applied)
			require.Nil(t, cloneAppliedLocalSet(syncer.appliedLocal))
			require.Empty(t, source.upserts)
		})
	}
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

func TestChannelMetaSyncStartAllowsTransientLeaderlessSource(t *testing.T) {
	source := &fakeChannelMetaSource{listErr: raftcluster.ErrNoLeader}
	cluster := &fakeChannelMetaCluster{}
	syncer := &channelMetaSync{
		source:          source,
		cluster:         cluster,
		localNode:       2,
		refreshInterval: time.Hour,
	}

	require.NoError(t, syncer.Start())
	require.NoError(t, syncer.Stop())
	require.Empty(t, cluster.applied)
	require.Empty(t, cluster.removed)
}

func TestChannelMetaBootstrapperDerivesInitialMetaFromSlotTopology(t *testing.T) {
	now := time.UnixMilli(1_700_000_123_000).UTC()
	store := &fakeBootstrapStore{
		getErr: metadb.ErrNotFound,
		authoritativeMeta: metadb.ChannelRuntimeMeta{
			ChannelID:    "u2@u1",
			ChannelType:  1,
			ChannelEpoch: 1,
			LeaderEpoch:  1,
			Replicas:     []uint64{1, 2, 3},
			ISR:          []uint64{1, 2, 3},
			Leader:       2,
			MinISR:       2,
			Status:       uint8(channel.StatusActive),
			Features:     uint64(channel.MessageSeqFormatLegacyU32),
			LeaseUntilMS: now.Add(channelMetaBootstrapLease).UnixMilli(),
		},
	}
	cluster := &fakeBootstrapCluster{
		slotID: 9,
		peers:  []uint64{1, 2, 3},
		leader: 2,
	}

	bootstrapper := newChannelMetaBootstrapper(cluster, store, 2, func() time.Time { return now }, wklog.NewNop())

	meta, created, err := bootstrapper.EnsureChannelRuntimeMeta(context.Background(), channel.ChannelID{ID: "u2@u1", Type: 1})
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, store.authoritativeMeta, meta)
	require.Len(t, store.upserts, 1)
	require.Equal(t, []uint64{1, 2, 3}, store.upserts[0].Replicas)
	require.Equal(t, []uint64{1, 2, 3}, store.upserts[0].ISR)
	require.Equal(t, uint64(2), store.upserts[0].Leader)
	require.Equal(t, int64(2), store.upserts[0].MinISR)
	require.Equal(t, uint64(1), store.upserts[0].ChannelEpoch)
	require.Equal(t, uint64(1), store.upserts[0].LeaderEpoch)
	require.Equal(t, uint8(channel.StatusActive), store.upserts[0].Status)
	require.Equal(t, uint64(channel.MessageSeqFormatLegacyU32), store.upserts[0].Features)
	require.Equal(t, now.Add(channelMetaBootstrapLease).UnixMilli(), store.upserts[0].LeaseUntilMS)
}

func TestChannelMetaBootstrapperClampsMinISRForSingleReplica(t *testing.T) {
	store := &fakeBootstrapStore{
		getErr: metadb.ErrNotFound,
		authoritativeMeta: metadb.ChannelRuntimeMeta{
			ChannelID:   "solo",
			ChannelType: 1,
			Replicas:    []uint64{7},
			ISR:         []uint64{7},
			Leader:      7,
			MinISR:      1,
			Status:      uint8(channel.StatusActive),
			Features:    uint64(channel.MessageSeqFormatLegacyU32),
		},
	}
	cluster := &fakeBootstrapCluster{
		slotID: 3,
		peers:  []uint64{7},
		leader: 7,
	}

	bootstrapper := newChannelMetaBootstrapper(cluster, store, 2, time.Now, wklog.NewNop())

	meta, created, err := bootstrapper.EnsureChannelRuntimeMeta(context.Background(), channel.ChannelID{ID: "solo", Type: 1})
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, int64(1), meta.MinISR)
	require.Len(t, store.upserts, 1)
	require.Equal(t, int64(1), store.upserts[0].MinISR)
}

func TestChannelMetaBootstrapperDefaultsMinISRToTwo(t *testing.T) {
	store := &fakeBootstrapStore{
		getErr: metadb.ErrNotFound,
		authoritativeMeta: metadb.ChannelRuntimeMeta{
			ChannelID:   "defaulted",
			ChannelType: 1,
			Replicas:    []uint64{4, 5, 6},
			ISR:         []uint64{4, 5, 6},
			Leader:      5,
			MinISR:      2,
			Status:      uint8(channel.StatusActive),
			Features:    uint64(channel.MessageSeqFormatLegacyU32),
		},
	}
	cluster := &fakeBootstrapCluster{
		slotID: 7,
		peers:  []uint64{4, 5, 6},
		leader: 5,
	}

	bootstrapper := newChannelMetaBootstrapper(cluster, store, 0, time.Now, wklog.NewNop())

	meta, created, err := bootstrapper.EnsureChannelRuntimeMeta(context.Background(), channel.ChannelID{ID: "defaulted", Type: 1})
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, int64(2), meta.MinISR)
	require.Len(t, store.upserts, 1)
	require.Equal(t, int64(2), store.upserts[0].MinISR)
}

func TestChannelMetaBootstrapperFailsWhenSlotHasNoPeers(t *testing.T) {
	store := &fakeBootstrapStore{getErr: metadb.ErrNotFound}
	cluster := &fakeBootstrapCluster{slotID: 5}
	bootstrapper := newChannelMetaBootstrapper(cluster, store, 2, time.Now, wklog.NewNop())

	_, created, err := bootstrapper.EnsureChannelRuntimeMeta(context.Background(), channel.ChannelID{ID: "empty", Type: 1})
	require.Error(t, err)
	require.ErrorContains(t, err, "slot peers")
	require.False(t, created)
	require.Empty(t, store.upserts)
}

func TestChannelMetaBootstrapperPropagatesRetryableClusterErrors(t *testing.T) {
	testCases := []struct {
		name string
		err  error
	}{
		{name: "no leader", err: raftcluster.ErrNoLeader},
		{name: "slot not found", err: raftcluster.ErrSlotNotFound},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeBootstrapStore{getErr: metadb.ErrNotFound}
			cluster := &fakeBootstrapCluster{
				slotID:    11,
				peers:     []uint64{1, 2, 3},
				leaderErr: tt.err,
			}
			bootstrapper := newChannelMetaBootstrapper(cluster, store, 2, time.Now, wklog.NewNop())

			_, created, err := bootstrapper.EnsureChannelRuntimeMeta(context.Background(), channel.ChannelID{ID: "unstable", Type: 1})
			require.ErrorIs(t, err, tt.err)
			require.False(t, created)
			require.Empty(t, store.upserts)
		})
	}
}

func TestChannelMetaBootstrapperLogsMissingBootstrappedAndFailedEvents(t *testing.T) {
	logger := newBootstrapRecordingLogger("app")
	id := channel.ChannelID{ID: "u2@u1", Type: 1}

	successStore := &fakeBootstrapStore{
		getErr: metadb.ErrNotFound,
		authoritativeMeta: metadb.ChannelRuntimeMeta{
			ChannelID:   id.ID,
			ChannelType: int64(id.Type),
			Replicas:    []uint64{1, 2, 3},
			ISR:         []uint64{1, 2, 3},
			Leader:      2,
			MinISR:      2,
			Status:      uint8(channel.StatusActive),
			Features:    uint64(channel.MessageSeqFormatLegacyU32),
		},
	}
	successCluster := &fakeBootstrapCluster{
		slotID: 21,
		peers:  []uint64{1, 2, 3},
		leader: 2,
	}
	successBootstrapper := newChannelMetaBootstrapper(successCluster, successStore, 2, time.Now, logger)

	_, created, err := successBootstrapper.EnsureChannelRuntimeMeta(context.Background(), id)
	require.NoError(t, err)
	require.True(t, created)

	failureStore := &fakeBootstrapStore{getErr: metadb.ErrNotFound}
	failureCluster := &fakeBootstrapCluster{
		slotID: 22,
		peers:  []uint64{9, 10},
		leader: 9,
	}
	failureBootstrapper := newChannelMetaBootstrapper(failureCluster, failureStore, 2, time.Now, logger)
	failureStore.upsertErr = errors.New("write failed")

	_, created, err = failureBootstrapper.EnsureChannelRuntimeMeta(context.Background(), channel.ChannelID{ID: "failed", Type: 2})
	require.Error(t, err)
	require.False(t, created)

	entries := logger.entries()
	require.Len(t, entries, 4)

	require.Equal(t, "INFO", entries[0].level)
	require.Equal(t, "missing runtime metadata; bootstrapping", entries[0].msg)
	requireBootstrapFieldEquals(t, entries[0], "event", "app.channelmeta.bootstrap.missing")
	requireBootstrapFieldEquals(t, entries[0], "channelID", "u2@u1")
	requireBootstrapFieldEquals(t, entries[0], "channelType", int64(1))
	requireBootstrapFieldEquals(t, entries[0], "slotID", uint64(21))
	requireBootstrapFieldEquals(t, entries[0], "leader", uint64(0))
	requireBootstrapFieldEquals(t, entries[0], "replicaCount", 3)
	requireBootstrapFieldEquals(t, entries[0], "minISR", int64(2))
	requireBootstrapFieldEquals(t, entries[0], "created", false)

	require.Equal(t, "INFO", entries[1].level)
	require.Equal(t, "bootstrapped runtime metadata", entries[1].msg)
	requireBootstrapFieldEquals(t, entries[1], "event", "app.channelmeta.bootstrap.bootstrapped")
	requireBootstrapFieldEquals(t, entries[1], "channelID", "u2@u1")
	requireBootstrapFieldEquals(t, entries[1], "channelType", int64(1))
	requireBootstrapFieldEquals(t, entries[1], "slotID", uint64(21))
	requireBootstrapFieldEquals(t, entries[1], "leader", uint64(2))
	requireBootstrapFieldEquals(t, entries[1], "replicaCount", 3)
	requireBootstrapFieldEquals(t, entries[1], "minISR", int64(2))
	requireBootstrapFieldEquals(t, entries[1], "created", true)

	require.Equal(t, "INFO", entries[2].level)
	require.Equal(t, "missing runtime metadata; bootstrapping", entries[2].msg)
	requireBootstrapFieldEquals(t, entries[2], "event", "app.channelmeta.bootstrap.missing")
	requireBootstrapFieldEquals(t, entries[2], "channelID", "failed")
	requireBootstrapFieldEquals(t, entries[2], "channelType", int64(2))
	requireBootstrapFieldEquals(t, entries[2], "slotID", uint64(22))
	requireBootstrapFieldEquals(t, entries[2], "leader", uint64(0))
	requireBootstrapFieldEquals(t, entries[2], "replicaCount", 2)
	requireBootstrapFieldEquals(t, entries[2], "minISR", int64(2))
	requireBootstrapFieldEquals(t, entries[2], "created", false)

	require.Equal(t, "ERROR", entries[3].level)
	require.Equal(t, "failed to bootstrap runtime metadata", entries[3].msg)
	requireBootstrapFieldEquals(t, entries[3], "event", "app.channelmeta.bootstrap.failed")
	requireBootstrapFieldEquals(t, entries[3], "channelID", "failed")
	requireBootstrapFieldEquals(t, entries[3], "channelType", int64(2))
	requireBootstrapFieldEquals(t, entries[3], "slotID", uint64(22))
	requireBootstrapFieldEquals(t, entries[3], "leader", uint64(9))
	requireBootstrapFieldEquals(t, entries[3], "replicaCount", 2)
	requireBootstrapFieldEquals(t, entries[3], "minISR", int64(2))
}

type fakeChannelMetaSource struct {
	get         map[channel.ChannelID]metadb.ChannelRuntimeMeta
	list        []metadb.ChannelRuntimeMeta
	getErr      error
	listErr     error
	version     uint64
	getResults  []fakeChannelMetaGetResult
	getCalls    int
	lastGetMeta metadb.ChannelRuntimeMeta
	upsertErr   error
	upserts     []metadb.ChannelRuntimeMeta
}

type fakeChannelMetaGetResult struct {
	meta metadb.ChannelRuntimeMeta
	err  error
}

func (f *fakeChannelMetaSource) GetChannelRuntimeMeta(_ context.Context, channelID string, channelType int64) (metadb.ChannelRuntimeMeta, error) {
	if f.getCalls < len(f.getResults) {
		result := f.getResults[f.getCalls]
		f.getCalls++
		if result.err != nil {
			return metadb.ChannelRuntimeMeta{}, result.err
		}
		f.lastGetMeta = result.meta
		return result.meta, nil
	}
	if f.getErr != nil {
		return metadb.ChannelRuntimeMeta{}, f.getErr
	}
	meta := f.get[channel.ChannelID{ID: channelID, Type: uint8(channelType)}]
	f.lastGetMeta = meta
	return meta, nil
}

func (f *fakeChannelMetaSource) ListChannelRuntimeMeta(context.Context) ([]metadb.ChannelRuntimeMeta, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]metadb.ChannelRuntimeMeta(nil), f.list...), nil
}

func (f *fakeChannelMetaSource) HashSlotTableVersion() uint64 {
	return f.version
}

func (f *fakeChannelMetaSource) UpsertChannelRuntimeMeta(_ context.Context, meta metadb.ChannelRuntimeMeta) error {
	f.upserts = append(f.upserts, meta)
	return f.upsertErr
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

type fakeBootstrapStore struct {
	getErr            error
	upsertErr         error
	authoritativeMeta metadb.ChannelRuntimeMeta
	upserts           []metadb.ChannelRuntimeMeta
}

func (f *fakeBootstrapStore) GetChannelRuntimeMeta(_ context.Context, _ string, _ int64) (metadb.ChannelRuntimeMeta, error) {
	if f.getErr != nil && len(f.upserts) == 0 {
		return metadb.ChannelRuntimeMeta{}, f.getErr
	}
	return f.authoritativeMeta, nil
}

func (f *fakeBootstrapStore) UpsertChannelRuntimeMeta(_ context.Context, meta metadb.ChannelRuntimeMeta) error {
	f.upserts = append(f.upserts, meta)
	return f.upsertErr
}

type fakeBootstrapCluster struct {
	slotID      multiraft.SlotID
	peers       []uint64
	leader      uint64
	leaderErr   error
	leaderCalls int
}

func (f *fakeBootstrapCluster) SlotForKey(string) multiraft.SlotID {
	return f.slotID
}

func (f *fakeBootstrapCluster) PeersForSlot(multiraft.SlotID) []multiraft.NodeID {
	out := make([]multiraft.NodeID, 0, len(f.peers))
	for _, peer := range f.peers {
		out = append(out, multiraft.NodeID(peer))
	}
	return out
}

func (f *fakeBootstrapCluster) LeaderOf(multiraft.SlotID) (multiraft.NodeID, error) {
	f.leaderCalls++
	if f.leaderErr != nil {
		return 0, f.leaderErr
	}
	return multiraft.NodeID(f.leader), nil
}

type bootstrapRecordedLogEntry struct {
	level  string
	module string
	msg    string
	fields []wklog.Field
}

func (e bootstrapRecordedLogEntry) field(key string) (wklog.Field, bool) {
	for _, field := range e.fields {
		if field.Key == key {
			return field, true
		}
	}
	return wklog.Field{}, false
}

type bootstrapRecordingLoggerSink struct {
	mu      sync.Mutex
	entries []bootstrapRecordedLogEntry
}

type bootstrapRecordingLogger struct {
	module string
	base   []wklog.Field
	sink   *bootstrapRecordingLoggerSink
}

func newBootstrapRecordingLogger(module string) *bootstrapRecordingLogger {
	return &bootstrapRecordingLogger{module: module, sink: &bootstrapRecordingLoggerSink{}}
}

func (r *bootstrapRecordingLogger) Debug(msg string, fields ...wklog.Field) {
	r.log("DEBUG", msg, fields...)
}
func (r *bootstrapRecordingLogger) Info(msg string, fields ...wklog.Field) {
	r.log("INFO", msg, fields...)
}
func (r *bootstrapRecordingLogger) Warn(msg string, fields ...wklog.Field) {
	r.log("WARN", msg, fields...)
}
func (r *bootstrapRecordingLogger) Error(msg string, fields ...wklog.Field) {
	r.log("ERROR", msg, fields...)
}
func (r *bootstrapRecordingLogger) Fatal(msg string, fields ...wklog.Field) {
	r.log("FATAL", msg, fields...)
}

func (r *bootstrapRecordingLogger) Named(name string) wklog.Logger {
	if name == "" {
		return r
	}
	module := name
	if r.module != "" {
		module = r.module + "." + name
	}
	return &bootstrapRecordingLogger{module: module, base: append([]wklog.Field(nil), r.base...), sink: r.sink}
}

func (r *bootstrapRecordingLogger) With(fields ...wklog.Field) wklog.Logger {
	return &bootstrapRecordingLogger{
		module: r.module,
		base:   append(append([]wklog.Field(nil), r.base...), fields...),
		sink:   r.sink,
	}
}

func (r *bootstrapRecordingLogger) Sync() error { return nil }

func (r *bootstrapRecordingLogger) log(level, msg string, fields ...wklog.Field) {
	if r == nil || r.sink == nil {
		return
	}
	r.sink.mu.Lock()
	defer r.sink.mu.Unlock()
	r.sink.entries = append(r.sink.entries, bootstrapRecordedLogEntry{
		level:  level,
		module: r.module,
		msg:    msg,
		fields: append(append([]wklog.Field(nil), r.base...), fields...),
	})
}

func (r *bootstrapRecordingLogger) entries() []bootstrapRecordedLogEntry {
	r.sink.mu.Lock()
	defer r.sink.mu.Unlock()
	out := make([]bootstrapRecordedLogEntry, len(r.sink.entries))
	copy(out, r.sink.entries)
	return out
}

func requireBootstrapFieldEquals(t *testing.T, entry bootstrapRecordedLogEntry, key string, want any) {
	t.Helper()
	field, ok := entry.field(key)
	require.True(t, ok, "missing log field %s", key)
	require.Equal(t, want, field.Value)
}
