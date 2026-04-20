package runtime

import (
	"context"
	"testing"
	"time"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/stretchr/testify/require"
)

func TestRuntimeLongPollServeLanePollTimesOutWhenLaneHasNoReadyItems(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.ReplicationMode = "long_poll"
		cfg.LongPollLaneCount = 4
		cfg.LongPollMaxWait = time.Millisecond
		cfg.LongPollMaxBytes = 64 * 1024
		cfg.LongPollMaxChannels = 64
	})
	meta := testMetaLocal(2701, 4, 1, []core.NodeID{1, 2})
	mustEnsureLocal(t, env.runtime, meta)

	resp, err := env.runtime.ServeLanePoll(context.Background(), LanePollRequestEnvelope{
		ReplicaID:   2,
		LaneID:      testFollowerLaneFor(meta.Key, 4),
		LaneCount:   4,
		Op:          LanePollOpOpen,
		MaxWait:     time.Millisecond,
		MaxBytes:    64 * 1024,
		MaxChannels: 64,
		FullMembership: []LaneMembership{
			{ChannelKey: meta.Key, ChannelEpoch: meta.Epoch},
		},
	})
	require.NoError(t, err)
	require.Equal(t, LanePollStatusOK, resp.Status)
	require.True(t, resp.TimedOut)
	require.NotZero(t, resp.SessionID)
	require.NotZero(t, resp.SessionEpoch)
}

func TestApplyMetaUpdatesLaneTargetsForLeaderChannels(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.ReplicationMode = "long_poll"
		cfg.LongPollLaneCount = 4
		cfg.LongPollMaxWait = time.Millisecond
		cfg.LongPollMaxBytes = 64 * 1024
		cfg.LongPollMaxChannels = 64
	})
	meta := testMetaLocal(2702, 4, 1, []core.NodeID{1, 2, 3})
	mustEnsureLocal(t, env.runtime, meta)

	ch, ok := env.runtime.lookupChannel(meta.Key)
	require.True(t, ok)
	require.Len(t, ch.replicationTargetsSnapshot(), 2)

	require.NoError(t, env.runtime.ApplyMeta(testMetaLocal(2702, 5, 3, []core.NodeID{1, 2, 3})))
	require.Empty(t, ch.replicationTargetsSnapshot())

	require.NoError(t, env.runtime.ApplyMeta(testMetaLocal(2702, 6, 1, []core.NodeID{1, 2, 3})))
	require.Len(t, ch.replicationTargetsSnapshot(), 2)
}

func TestRuntimeLongPollLeaderAppendWakesParkedLanePoll(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.ReplicationMode = "long_poll"
		cfg.LongPollLaneCount = 4
		cfg.LongPollMaxWait = time.Second
		cfg.LongPollMaxBytes = 64 * 1024
		cfg.LongPollMaxChannels = 64
	})
	meta := testMetaLocal(2703, 4, 1, []core.NodeID{1, 2})
	mustEnsureLocal(t, env.runtime, meta)

	handle, ok := env.runtime.Channel(meta.Key)
	require.True(t, ok)

	laneID := testFollowerLaneFor(meta.Key, 4)
	respCh := make(chan struct {
		resp LanePollResponseEnvelope
		err  error
	}, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go func() {
		resp, err := env.runtime.ServeLanePoll(ctx, LanePollRequestEnvelope{
			ReplicaID:   2,
			LaneID:      laneID,
			LaneCount:   4,
			Op:          LanePollOpOpen,
			MaxWait:     time.Second,
			MaxBytes:    64 * 1024,
			MaxChannels: 64,
			FullMembership: []LaneMembership{
				{ChannelKey: meta.Key, ChannelEpoch: meta.Epoch},
			},
		})
		respCh <- struct {
			resp LanePollResponseEnvelope
			err  error
		}{resp: resp, err: err}
	}()

	require.Eventually(t, func() bool {
		session, ok := env.runtime.leaderLanes.Session(PeerLaneKey{Peer: 2, LaneID: laneID})
		if !ok {
			return false
		}
		session.mu.Lock()
		defer session.mu.Unlock()
		return session.parked != nil
	}, time.Second, time.Millisecond)

	replica := env.factory.replicas[0]
	replica.mu.Lock()
	replica.state.LEO = 1
	replica.fetchResult = core.ReplicaFetchResult{
		HW: 1,
		Records: []core.Record{
			{Payload: []byte("wake"), SizeBytes: len("wake")},
		},
	}
	replica.mu.Unlock()

	_, err := handle.Append(context.Background(), []core.Record{
		{Payload: []byte("wake"), SizeBytes: len("wake")},
	})
	require.NoError(t, err)

	select {
	case got := <-respCh:
		require.NoError(t, got.err)
		require.False(t, got.resp.TimedOut)
		require.Len(t, got.resp.Items, 1)
		require.Equal(t, meta.Key, got.resp.Items[0].ChannelKey)
		require.Equal(t, LanePollItemFlagData, got.resp.Items[0].Flags)
	case <-time.After(time.Second):
		t.Fatal("expected parked long poll to wake after leader append")
	}
}

func TestRuntimeLongPollInitialOpenReturnsAlreadyAppendedData(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.ReplicationMode = "long_poll"
		cfg.LongPollLaneCount = 4
		cfg.LongPollMaxWait = time.Millisecond
		cfg.LongPollMaxBytes = 64 * 1024
		cfg.LongPollMaxChannels = 64
	})
	meta := testMetaLocal(2704, 4, 1, []core.NodeID{1, 2})
	mustEnsureLocal(t, env.runtime, meta)

	handle, ok := env.runtime.Channel(meta.Key)
	require.True(t, ok)

	replica := env.factory.replicas[0]
	replica.mu.Lock()
	replica.state.LEO = 1
	replica.fetchResult = core.ReplicaFetchResult{
		HW: 1,
		Records: []core.Record{
			{Payload: []byte("first"), SizeBytes: len("first")},
		},
	}
	replica.mu.Unlock()

	_, err := handle.Append(context.Background(), []core.Record{
		{Payload: []byte("first"), SizeBytes: len("first")},
	})
	require.NoError(t, err)

	resp, err := env.runtime.ServeLanePoll(context.Background(), LanePollRequestEnvelope{
		ReplicaID:   2,
		LaneID:      testFollowerLaneFor(meta.Key, 4),
		LaneCount:   4,
		Op:          LanePollOpOpen,
		MaxWait:     time.Millisecond,
		MaxBytes:    64 * 1024,
		MaxChannels: 64,
		FullMembership: []LaneMembership{
			{ChannelKey: meta.Key, ChannelEpoch: meta.Epoch},
		},
	})
	require.NoError(t, err)
	require.Equal(t, LanePollStatusOK, resp.Status)
	require.False(t, resp.TimedOut)
	require.Len(t, resp.Items, 1)
	require.Equal(t, meta.Key, resp.Items[0].ChannelKey)
	require.Equal(t, LanePollItemFlagData, resp.Items[0].Flags)
	require.Len(t, resp.Items[0].Records, 1)
}

func TestRuntimeLongPollReopenReturnsAlreadyAppendedData(t *testing.T) {
	env := newSessionTestEnvWithConfig(t, func(cfg *Config) {
		cfg.ReplicationMode = "long_poll"
		cfg.LongPollLaneCount = 4
		cfg.LongPollMaxWait = time.Millisecond
		cfg.LongPollMaxBytes = 64 * 1024
		cfg.LongPollMaxChannels = 64
	})
	meta := testMetaLocal(2705, 4, 1, []core.NodeID{1, 2})
	mustEnsureLocal(t, env.runtime, meta)

	handle, ok := env.runtime.Channel(meta.Key)
	require.True(t, ok)

	replica := env.factory.replicas[0]
	replica.mu.Lock()
	replica.state.LEO = 1
	replica.fetchResult = core.ReplicaFetchResult{
		HW: 1,
		Records: []core.Record{
			{Payload: []byte("reopen"), SizeBytes: len("reopen")},
		},
	}
	replica.mu.Unlock()

	_, err := handle.Append(context.Background(), []core.Record{
		{Payload: []byte("reopen"), SizeBytes: len("reopen")},
	})
	require.NoError(t, err)

	resp, err := env.runtime.ServeLanePoll(context.Background(), LanePollRequestEnvelope{
		ReplicaID:   2,
		LaneID:      testFollowerLaneFor(meta.Key, 4),
		LaneCount:   4,
		SessionID:   701,
		SessionEpoch: 1,
		Op:          LanePollOpOpen,
		MaxWait:     time.Millisecond,
		MaxBytes:    64 * 1024,
		MaxChannels: 64,
		FullMembership: []LaneMembership{
			{ChannelKey: meta.Key, ChannelEpoch: meta.Epoch},
		},
	})
	require.NoError(t, err)
	require.Equal(t, LanePollStatusOK, resp.Status)
	require.False(t, resp.TimedOut)
	require.Len(t, resp.Items, 1)
	require.Equal(t, meta.Key, resp.Items[0].ChannelKey)
	require.Equal(t, LanePollItemFlagData, resp.Items[0].Flags)
	require.Len(t, resp.Items[0].Records, 1)
}
