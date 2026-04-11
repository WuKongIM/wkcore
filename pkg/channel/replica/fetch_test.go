package replica

import (
	"context"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/stretchr/testify/require"
)

func TestFetchRejectsInvalidBudget(t *testing.T) {
	r := newLeaderReplica(t)
	_, err := r.Fetch(context.Background(), channel.ReplicaFetchRequest{
		ChannelKey:  "group-10",
		Epoch:       7,
		ReplicaID:   2,
		FetchOffset: 0,
		OffsetEpoch: 3,
		MaxBytes:    0,
	})
	require.ErrorIs(t, err, channel.ErrInvalidFetchBudget)
}

func TestFetchRejectsMismatchedChannelKey(t *testing.T) {
	r := newLeaderReplica(t)
	_, err := r.Fetch(context.Background(), channel.ReplicaFetchRequest{
		ChannelKey:  "group-other",
		Epoch:       7,
		ReplicaID:   2,
		FetchOffset: 0,
		OffsetEpoch: 3,
		MaxBytes:    1024,
	})
	require.ErrorIs(t, err, channel.ErrStaleMeta)
}

func TestFetchReturnsTruncateToWhenOffsetEpochDiverges(t *testing.T) {
	env := newFetchEnvWithHistory(t)
	result, err := env.replica.Fetch(context.Background(), channel.ReplicaFetchRequest{
		ChannelKey:  "group-10",
		Epoch:       7,
		ReplicaID:   2,
		FetchOffset: 5,
		OffsetEpoch: 4,
		MaxBytes:    1024,
	})
	require.NoError(t, err)
	require.NotNil(t, result.TruncateTo)
	require.Equal(t, uint64(4), *result.TruncateTo)
}

func TestFetchReturnsSnapshotRequiredWhenFollowerFallsBehindLogStart(t *testing.T) {
	env := newFetchEnvWithHistory(t)
	env.replica.state.LogStartOffset = 4
	env.replica.publishStateLocked()
	_, err := env.replica.Fetch(context.Background(), channel.ReplicaFetchRequest{
		ChannelKey:  "group-10",
		Epoch:       7,
		ReplicaID:   2,
		FetchOffset: 3,
		OffsetEpoch: 3,
		MaxBytes:    1024,
	})
	require.ErrorIs(t, err, channel.ErrSnapshotRequired)
}
