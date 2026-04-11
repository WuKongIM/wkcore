package replica

import (
	"context"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/stretchr/testify/require"
)

func TestStatusReturnsLatestReplicaSnapshot(t *testing.T) {
	r := newLeaderReplica(t)
	_, err := r.Append(context.Background(), []channel.Record{{Payload: []byte("a"), SizeBytes: 1}})
	require.NoError(t, err)
	require.Equal(t, uint64(1), r.Status().LEO)
}

func TestStatusIsUpdatedAfterFollowerAckAdvancesHW(t *testing.T) {
	cluster := newThreeReplicaCluster(t)
	done := make(chan channel.CommitResult, 1)
	go func() {
		res, err := cluster.leader.Append(context.Background(), []channel.Record{{Payload: []byte("m"), SizeBytes: 1}})
		if err == nil {
			done <- res
		}
	}()
	waitForLogAppend(t, cluster.leader.log.(*fakeLogStore), 1)

	cluster.replicateOnce(t, cluster.follower2)
	cluster.replicateOnce(t, cluster.follower3)
	<-done

	state := cluster.leader.Status()
	require.Equal(t, uint64(1), state.HW)
	require.Equal(t, uint64(1), state.LEO)
}
