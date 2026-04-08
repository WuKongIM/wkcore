package isr

import (
	"context"
	"errors"
	"testing"
	"time"
)

type staleAppendLEOLogStore struct {
	*fakeLogStore
}

func (f *staleAppendLEOLogStore) LEO() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.records) > 0 {
		return 0
	}
	return f.leo
}

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
	waitForLogAppend(t, env.leader.log.(*fakeLogStore), 1)

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

func TestAppendFailsFastWhenMetaISRBelowMinISR(t *testing.T) {
	env := newTestEnv(t)
	meta := activeMetaWithMinISR(7, 1, 3)
	meta.ISR = []NodeID{1, 3}

	env.replica = newReplicaFromEnv(t, env)
	env.replica.mustApplyMeta(t, meta)
	if err := env.replica.BecomeLeader(meta); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	_, err := env.replica.Append(context.Background(), []Record{{Payload: []byte("x"), SizeBytes: 1}})
	if !errors.Is(err, ErrInsufficientISR) {
		t.Fatalf("expected ErrInsufficientISR, got %v", err)
	}
}

func TestAppendCommitsUsingReturnedBaseOffsetWhenLEOReadLags(t *testing.T) {
	env := newTestEnv(t)

	logStore := &staleAppendLEOLogStore{
		fakeLogStore: &fakeLogStore{},
	}
	got, err := NewReplica(ReplicaConfig{
		LocalNode:         env.localNode,
		LogStore:          logStore,
		CheckpointStore:   env.checkpoints,
		EpochHistoryStore: env.history,
		SnapshotApplier:   env.snapshots,
		Now:               env.clock.Now,
	})
	if err != nil {
		t.Fatalf("NewReplica() error = %v", err)
	}

	r, ok := got.(*replica)
	if !ok {
		t.Fatalf("NewReplica() type = %T", got)
	}
	meta := activeMetaWithMinISR(7, 1, 1)
	r.mustApplyMeta(t, meta)
	if err := r.BecomeLeader(meta); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	result, err := r.Append(ctx, []Record{{Payload: []byte("x"), SizeBytes: 1}})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if result.BaseOffset != 0 || result.NextCommitHW != 1 {
		t.Fatalf("result = %+v", result)
	}
	if r.state.LEO != 1 || r.state.HW != 1 {
		t.Fatalf("state = %+v", r.state)
	}
}
