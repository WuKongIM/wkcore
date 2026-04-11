package replica

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/stretchr/testify/require"
)

func TestAppendCollectorDrainsBurstsWithoutRetrigger(t *testing.T) {
	r := newLeaderReplica(t)
	ctx := context.Background()

	_, err := r.Append(ctx, []channel.Record{{Payload: []byte("a"), SizeBytes: 1}})
	require.NoError(t, err)

	_, err = r.Append(ctx, []channel.Record{{Payload: []byte("b"), SizeBytes: 1}})
	require.NoError(t, err)

	state := r.Status()
	require.GreaterOrEqual(t, state.LEO, uint64(2))
}

func TestAppendRejectsReplicaThatIsNotLeader(t *testing.T) {
	r := newFollowerReplica(t)
	_, err := r.Append(context.Background(), []channel.Record{{Payload: []byte("x"), SizeBytes: 1}})
	require.ErrorIs(t, err, channel.ErrNotLeader)
}

func TestAppendWaitsUntilMinISRReplicasAcknowledgeViaFetch(t *testing.T) {
	env := newThreeReplicaCluster(t)
	done := make(chan channel.CommitResult, 1)

	go func() {
		res, err := env.leader.Append(context.Background(), []channel.Record{{Payload: []byte("x"), SizeBytes: 1}})
		if err == nil {
			done <- res
		}
	}()
	waitForLogAppend(t, env.leader.log.(*fakeLogStore), 1)

	env.replicateOnce(t, env.follower2)
	select {
	case <-done:
		t.Fatal("append returned before MinISR was satisfied")
	default:
	}

	env.replicateOnce(t, env.follower3)
	res := <-done
	require.Equal(t, uint64(1), res.NextCommitHW)
}

func TestLeaderLeaseExpiryFencesAppend(t *testing.T) {
	env := newTestEnv(t)
	meta := activeMeta(7, 1)
	meta.LeaseUntil = env.clock.Now().Add(time.Second)
	env.replica = newReplicaFromEnv(t, env)
	env.replica.mustApplyMeta(t, meta)
	require.NoError(t, env.replica.BecomeLeader(meta))

	env.clock.Advance(2 * time.Second)
	_, err := env.replica.Append(context.Background(), []channel.Record{{Payload: []byte("x"), SizeBytes: 1}})
	require.ErrorIs(t, err, channel.ErrLeaseExpired)
	require.Equal(t, channel.ReplicaRoleFencedLeader, env.replica.state.Role)
}

func TestAppendContextCancellationReturnsPromptly(t *testing.T) {
	env := newThreeReplicaCluster(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := env.leader.Append(ctx, []channel.Record{{Payload: []byte("x"), SizeBytes: 1}})
		done <- err
	}()
	waitForLogAppend(t, env.leader.log.(*fakeLogStore), 1)
	cancel()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Append() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("append did not return after context cancellation")
	}
}
