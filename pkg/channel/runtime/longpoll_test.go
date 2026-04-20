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
