package replica

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/stretchr/testify/require"
)

func TestStatusReturnsLatestReplicaSnapshot(t *testing.T) {
	cluster := newThreeReplicaCluster(t)
	done := make(chan channel.CommitResult, 1)
	go func() {
		res, err := cluster.leader.Append(context.Background(), []channel.Record{{Payload: []byte("a"), SizeBytes: 1}})
		if err == nil {
			done <- res
		}
	}()
	waitForLogAppend(t, cluster.leader.log.(*fakeLogStore), 1)

	deadline := time.After(time.Second)
	for {
		state := cluster.leader.Status()
		if state.LEO == 1 {
			require.Equal(t, uint64(0), state.HW)
			break
		}
		select {
		case <-deadline:
			t.Fatal("Status() did not expose latest LEO snapshot while append was waiting for quorum")
		default:
		}
	}
	select {
	case <-done:
		t.Fatal("append returned before MinISR quorum")
	default:
	}

	cluster.replicateOnce(t, cluster.follower2)
	cluster.replicateOnce(t, cluster.follower3)
	<-done
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
