package app

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isrnode"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/stretchr/testify/require"
)

func TestChannelMetaSyncRefreshProjectsLeaderEpochLeaseAndApplyOrder(t *testing.T) {
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
		Status:       uint8(channellog.ChannelStatusActive),
		Features:     uint64(channellog.MessageSeqFormatU64),
		LeaseUntilMS: leaseUntil.UnixMilli(),
	}

	var calls []string
	source := &fakeChannelMetaSource{
		get: map[channellog.ChannelKey]metadb.ChannelRuntimeMeta{
			{ChannelID: "u1", ChannelType: 1}: meta,
		},
	}
	runtime := newFakeChannelRuntime(&calls)
	cluster := newFakeChannelMetaCluster(&calls)
	syncer := &channelMetaSync{
		source:    source,
		runtime:   runtime,
		cluster:   cluster,
		localNode: 2,
	}

	got, err := syncer.RefreshChannelMeta(context.Background(), channellog.ChannelKey{
		ChannelID:   "u1",
		ChannelType: 1,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"runtime.ensure", "cluster.apply"}, calls)
	require.Len(t, runtime.ensured, 1)
	require.Empty(t, runtime.applied)
	require.Equal(t, uint64(7), runtime.ensured[0].Epoch)
	require.Equal(t, leaseUntil, runtime.ensured[0].LeaseUntil)
	require.Len(t, cluster.applied, 1)
	require.Equal(t, cluster.applied[0], got)
	require.Equal(t, channellog.MessageSeqFormatU64, got.Features.MessageSeqFormat)
}

func TestChannelMetaSyncRefreshUpdatesExistingGroupBeforeChannelLog(t *testing.T) {
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    "u2",
		ChannelType:  1,
		ChannelEpoch: 8,
		LeaderEpoch:  9,
		Replicas:     []uint64{2, 4},
		ISR:          []uint64{2, 4},
		Leader:       4,
		MinISR:       1,
		Status:       uint8(channellog.ChannelStatusActive),
		Features:     uint64(channellog.MessageSeqFormatLegacyU32),
		LeaseUntilMS: time.UnixMilli(1_700_000_456_000).UnixMilli(),
	}

	var calls []string
	source := &fakeChannelMetaSource{
		get: map[channellog.ChannelKey]metadb.ChannelRuntimeMeta{
			{ChannelID: "u2", ChannelType: 1}: meta,
		},
	}
	runtime := newFakeChannelRuntime(&calls)
	runtime.groups[channellog.GroupKeyForChannel(channellog.ChannelKey{ChannelID: "u2", ChannelType: 1})] = struct{}{}
	cluster := newFakeChannelMetaCluster(&calls)
	syncer := &channelMetaSync{
		source:    source,
		runtime:   runtime,
		cluster:   cluster,
		localNode: 2,
	}

	_, err := syncer.RefreshChannelMeta(context.Background(), channellog.ChannelKey{
		ChannelID:   "u2",
		ChannelType: 1,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"runtime.apply", "cluster.apply"}, calls)
	require.Empty(t, runtime.ensured)
	require.Len(t, runtime.applied, 1)
	require.Equal(t, uint64(9), runtime.applied[0].Epoch)
}

func TestChannelMetaSyncSyncOncePreloadsOnlyLocalReplicaMetas(t *testing.T) {
	var calls []string
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
				Status:       uint8(channellog.ChannelStatusActive),
				Features:     uint64(channellog.MessageSeqFormatLegacyU32),
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
				Status:       uint8(channellog.ChannelStatusActive),
				Features:     uint64(channellog.MessageSeqFormatLegacyU32),
			},
		},
	}
	runtime := newFakeChannelRuntime(&calls)
	cluster := newFakeChannelMetaCluster(&calls)
	syncer := &channelMetaSync{
		source:    source,
		runtime:   runtime,
		cluster:   cluster,
		localNode: 2,
	}

	require.NoError(t, syncer.syncOnce(context.Background()))
	require.Len(t, runtime.ensured, 1)
	require.Equal(t, "local", cluster.applied[0].ChannelID)
}

func TestChannelMetaSyncRefreshRejectsNonLocalReplicaMeta(t *testing.T) {
	source := &fakeChannelMetaSource{
		get: map[channellog.ChannelKey]metadb.ChannelRuntimeMeta{
			{ChannelID: "remote", ChannelType: 1}: {
				ChannelID:    "remote",
				ChannelType:  1,
				ChannelEpoch: 2,
				LeaderEpoch:  3,
				Replicas:     []uint64{7, 8},
				ISR:          []uint64{7, 8},
				Leader:       7,
				MinISR:       1,
				Status:       uint8(channellog.ChannelStatusActive),
				Features:     uint64(channellog.MessageSeqFormatLegacyU32),
			},
		},
	}
	runtime := newFakeChannelRuntime(nil)
	cluster := newFakeChannelMetaCluster(nil)
	syncer := &channelMetaSync{
		source:    source,
		runtime:   runtime,
		cluster:   cluster,
		localNode: 2,
	}

	_, err := syncer.RefreshChannelMeta(context.Background(), channellog.ChannelKey{
		ChannelID:   "remote",
		ChannelType: 1,
	})
	require.ErrorIs(t, err, channellog.ErrStaleMeta)
	require.Empty(t, cluster.applied)
	require.Empty(t, runtime.ensured)
	require.Empty(t, runtime.applied)
}

func TestChannelMetaSyncSyncOnceRemovesGroupsAndMetaNoLongerAssignedLocally(t *testing.T) {
	var calls []string
	key := channellog.ChannelKey{ChannelID: "local", ChannelType: 1}
	groupKey := channellog.GroupKeyForChannel(key)
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
				Status:       uint8(channellog.ChannelStatusActive),
				Features:     uint64(channellog.MessageSeqFormatLegacyU32),
			},
		},
	}
	runtime := newFakeChannelRuntime(&calls)
	runtime.groups[groupKey] = struct{}{}
	cluster := newFakeChannelMetaCluster(&calls)
	syncer := &channelMetaSync{
		source:    source,
		runtime:   runtime,
		cluster:   cluster,
		localNode: 2,
		appliedLocal: map[channellog.ChannelKey]struct{}{
			key: {},
		},
	}

	require.NoError(t, syncer.syncOnce(context.Background()))
	require.Equal(t, []string{"runtime.remove", "cluster.remove"}, calls)
	require.Equal(t, []isr.GroupKey{groupKey}, runtime.removed)
	require.Equal(t, []channellog.ChannelKey{key}, cluster.removed)
}

func TestChannelMetaSyncStopRemovesAppliedLocalGroups(t *testing.T) {
	var calls []string
	key := channellog.ChannelKey{ChannelID: "local", ChannelType: 1}
	groupKey := channellog.GroupKeyForChannel(key)

	runtime := newFakeChannelRuntime(&calls)
	runtime.groups[groupKey] = struct{}{}
	cluster := newFakeChannelMetaCluster(&calls)
	done := make(chan struct{})
	syncer := &channelMetaSync{
		runtime:   runtime,
		cluster:   cluster,
		localNode: 2,
		cancel: func() {
			close(done)
		},
		done: done,
		appliedLocal: map[channellog.ChannelKey]struct{}{
			key: {},
		},
	}

	require.NoError(t, syncer.Stop())
	require.Equal(t, []string{"runtime.remove", "cluster.remove"}, calls)
	require.Equal(t, []isr.GroupKey{groupKey}, runtime.removed)
	require.Equal(t, []channellog.ChannelKey{key}, cluster.removed)
	require.Nil(t, syncer.snapshotAppliedLocal())
}

func TestChannelMetaSyncStopRemovesRuntimeGroupLeftByClusterApplyFailure(t *testing.T) {
	var calls []string
	key := channellog.ChannelKey{ChannelID: "local", ChannelType: 1}
	meta := metadb.ChannelRuntimeMeta{
		ChannelID:    key.ChannelID,
		ChannelType:  int64(key.ChannelType),
		ChannelEpoch: 1,
		LeaderEpoch:  2,
		Replicas:     []uint64{2, 3},
		ISR:          []uint64{2, 3},
		Leader:       2,
		MinISR:       2,
		Status:       uint8(channellog.ChannelStatusActive),
		Features:     uint64(channellog.MessageSeqFormatLegacyU32),
	}
	source := &fakeChannelMetaSource{
		get: map[channellog.ChannelKey]metadb.ChannelRuntimeMeta{
			key: meta,
		},
	}
	runtime := newFakeChannelRuntime(&calls)
	cluster := newFakeChannelMetaCluster(&calls)
	cluster.applyErr = channellog.ErrConflictingMeta
	done := make(chan struct{})
	syncer := &channelMetaSync{
		source:    source,
		runtime:   runtime,
		cluster:   cluster,
		localNode: 2,
		cancel: func() {
			close(done)
		},
		done: done,
	}

	_, err := syncer.RefreshChannelMeta(context.Background(), key)
	require.ErrorIs(t, err, channellog.ErrConflictingMeta)

	require.NoError(t, syncer.Stop())
	require.Equal(t, []string{"runtime.ensure", "cluster.apply", "runtime.remove", "cluster.remove"}, calls)
	require.Equal(t, []isr.GroupKey{channellog.GroupKeyForChannel(key)}, runtime.removed)
	require.Equal(t, []channellog.ChannelKey{key}, cluster.removed)
}

type fakeChannelMetaSource struct {
	get  map[channellog.ChannelKey]metadb.ChannelRuntimeMeta
	list []metadb.ChannelRuntimeMeta
}

func (f *fakeChannelMetaSource) GetChannelRuntimeMeta(_ context.Context, channelID string, channelType int64) (metadb.ChannelRuntimeMeta, error) {
	return f.get[channellog.ChannelKey{ChannelID: channelID, ChannelType: uint8(channelType)}], nil
}

func (f *fakeChannelMetaSource) ListChannelRuntimeMeta(context.Context) ([]metadb.ChannelRuntimeMeta, error) {
	return append([]metadb.ChannelRuntimeMeta(nil), f.list...), nil
}

type fakeChannelRuntime struct {
	groups   map[isr.GroupKey]struct{}
	ensured  []isr.GroupMeta
	applied  []isr.GroupMeta
	removed  []isr.GroupKey
	callSink *[]string
}

func newFakeChannelRuntime(callSink *[]string) *fakeChannelRuntime {
	return &fakeChannelRuntime{
		groups:   make(map[isr.GroupKey]struct{}),
		callSink: callSink,
	}
}

func (f *fakeChannelRuntime) EnsureGroup(meta isr.GroupMeta) error {
	if f.callSink != nil {
		*f.callSink = append(*f.callSink, "runtime.ensure")
	}
	f.groups[meta.GroupKey] = struct{}{}
	f.ensured = append(f.ensured, meta)
	return nil
}

func (f *fakeChannelRuntime) RemoveGroup(groupKey isr.GroupKey) error {
	if f.callSink != nil {
		*f.callSink = append(*f.callSink, "runtime.remove")
	}
	f.removed = append(f.removed, groupKey)
	delete(f.groups, groupKey)
	return nil
}

func (f *fakeChannelRuntime) ApplyMeta(meta isr.GroupMeta) error {
	if f.callSink != nil {
		*f.callSink = append(*f.callSink, "runtime.apply")
	}
	f.applied = append(f.applied, meta)
	return nil
}

func (f *fakeChannelRuntime) Group(groupKey isr.GroupKey) (isrnode.GroupHandle, bool) {
	if _, ok := f.groups[groupKey]; !ok {
		return nil, false
	}
	return fakeRuntimeGroupHandle{}, true
}

func (f *fakeChannelRuntime) ServeFetch(context.Context, isrnode.FetchRequestEnvelope) (isrnode.FetchResponseEnvelope, error) {
	return isrnode.FetchResponseEnvelope{}, nil
}

type fakeRuntimeGroupHandle struct{}

func (fakeRuntimeGroupHandle) ID() isr.GroupKey {
	return ""
}

func (fakeRuntimeGroupHandle) Status() isr.ReplicaState {
	return isr.ReplicaState{}
}

func (fakeRuntimeGroupHandle) Append(context.Context, []isr.Record) (isr.CommitResult, error) {
	return isr.CommitResult{}, nil
}

type fakeChannelMetaCluster struct {
	applied  []channellog.ChannelMeta
	removed  []channellog.ChannelKey
	applyErr error
	callSink *[]string
}

func newFakeChannelMetaCluster(callSink *[]string) *fakeChannelMetaCluster {
	return &fakeChannelMetaCluster{callSink: callSink}
}

func (f *fakeChannelMetaCluster) ApplyMeta(meta channellog.ChannelMeta) error {
	if f.callSink != nil {
		*f.callSink = append(*f.callSink, "cluster.apply")
	}
	f.applied = append(f.applied, meta)
	return f.applyErr
}

func (f *fakeChannelMetaCluster) Send(context.Context, channellog.SendRequest) (channellog.SendResult, error) {
	return channellog.SendResult{}, nil
}

func (f *fakeChannelMetaCluster) Fetch(context.Context, channellog.FetchRequest) (channellog.FetchResult, error) {
	return channellog.FetchResult{}, nil
}

func (f *fakeChannelMetaCluster) Status(channellog.ChannelKey) (channellog.ChannelRuntimeStatus, error) {
	return channellog.ChannelRuntimeStatus{}, nil
}

func (f *fakeChannelMetaCluster) RemoveMeta(key channellog.ChannelKey) error {
	if f.callSink != nil {
		*f.callSink = append(*f.callSink, "cluster.remove")
	}
	f.removed = append(f.removed, key)
	return nil
}
