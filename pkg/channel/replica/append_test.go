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
	env := newTestEnv(t)
	env.replica = newReplicaFromEnvWithGroupCommit(t, env, time.Millisecond, 1, 1024)
	meta := activeMetaWithMinISR(7, 1, 1)
	env.replica.mustApplyMeta(t, meta)
	require.NoError(t, env.replica.BecomeLeader(meta))

	env.log.syncStarted = make(chan struct{}, 1)
	env.log.syncContinue = make(chan struct{})

	waiter1 := acquireAppendWaiter()
	waiter1.result = channel.CommitResult{RecordCount: 1}
	req1 := acquireAppendRequest()
	req1.ctx = context.Background()
	req1.batch = []channel.Record{{Payload: []byte("a"), SizeBytes: 1}}
	req1.byteCount = appendRequestBytes(req1.batch)
	req1.waiter = waiter1

	waiter2 := acquireAppendWaiter()
	waiter2.result = channel.CommitResult{RecordCount: 1}
	req2 := acquireAppendRequest()
	req2.ctx = context.Background()
	req2.batch = []channel.Record{{Payload: []byte("b"), SizeBytes: 1}}
	req2.byteCount = appendRequestBytes(req2.batch)
	req2.waiter = waiter2

	env.replica.appendMu.Lock()
	env.replica.appendPending = append(env.replica.appendPending, req1)
	env.replica.appendMu.Unlock()
	// Trigger collector only once. The second request is enqueued while collector
	// is flushing and must still be drained by the same collector loop.
	env.replica.signalAppendCollector()

	<-env.log.syncStarted
	env.replica.appendMu.Lock()
	env.replica.appendPending = append(env.replica.appendPending, req2)
	env.replica.appendMu.Unlock()
	close(env.log.syncContinue)

	select {
	case <-waiter1.ch:
	case <-time.After(time.Second):
		t.Fatal("first append request did not complete")
	}
	select {
	case <-waiter2.ch:
	case <-time.After(time.Second):
		t.Fatal("second append request did not complete without retrigger")
	}

	releaseAppendWaiter(waiter1)
	releaseAppendWaiter(waiter2)
	releaseAppendRequest(req1)
	releaseAppendRequest(req2)
	require.Equal(t, 2, env.log.appendCount)
	require.Equal(t, uint64(2), env.replica.Status().LEO)
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
