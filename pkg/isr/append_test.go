package isr

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAppendRejectsReplicaThatIsNotLeader(t *testing.T) {
	r := newFollowerReplica(t)

	_, err := r.Append(context.Background(), []Record{{Payload: []byte("x"), SizeBytes: 1}})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func TestAppendWaitsUntilMinISRReplicasAcknowledgeViaFetch(t *testing.T) {
	env := newThreeReplicaCluster(t)
	done := make(chan CommitResult, 1)

	go func() {
		res, err := env.leader.Append(context.Background(), []Record{{Payload: []byte("x"), SizeBytes: 1}})
		if err != nil {
			t.Errorf("Append() error = %v", err)
			return
		}
		done <- res
	}()
	waitForReplicaLEO(t, env.leader, 1)

	env.replicateOnce(t, env.follower2)
	if got := env.leader.progress[2]; got != 1 {
		t.Fatalf("progress[2] = %d", got)
	}
	if got := env.leader.state.HW; got != 0 {
		t.Fatalf("HW after follower2 = %d", got)
	}
	select {
	case <-done:
		t.Fatal("append returned before MinISR was satisfied")
	default:
	}

	env.replicateOnce(t, env.follower3)
	if got := env.leader.progress[3]; got != 1 {
		t.Fatalf("progress[3] = %d", got)
	}
	if got := env.leader.state.HW; got != 1 {
		t.Fatalf("HW after follower3 = %d", got)
	}
	res := <-done
	if res.NextCommitHW != 1 {
		t.Fatalf("NextCommitHW = %d", res.NextCommitHW)
	}
}

func TestLeaderLeaseExpiryFencesAppend(t *testing.T) {
	env := newTestEnv(t)
	meta := activeMeta(7, 1)
	meta.LeaseUntil = env.clock.Now().Add(time.Second)
	env.replica = newReplicaFromEnv(t, env)
	env.replica.mustApplyMeta(t, meta)
	if err := env.replica.BecomeLeader(meta); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	env.clock.Advance(2 * time.Second)
	syncCount := env.log.syncCount

	_, err := env.replica.Append(context.Background(), []Record{{Payload: []byte("x"), SizeBytes: 1}})
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}
	if env.replica.state.Role != RoleFencedLeader {
		t.Fatalf("role = %v", env.replica.state.Role)
	}
	if env.log.syncCount != syncCount {
		t.Fatalf("syncCount = %d, want %d", env.log.syncCount, syncCount)
	}

	_, err = env.replica.Append(context.Background(), []Record{{Payload: []byte("y"), SizeBytes: 1}})
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired on fenced leader, got %v", err)
	}
}
