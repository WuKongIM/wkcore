package isr

import (
	"context"
	"errors"
	"testing"
)

func TestApplyProgressAckAdvancesHWWithoutFollowUpFetch(t *testing.T) {
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

	replicateFollowerAndAck := func(follower *replica) {
		fetch, err := env.leader.Fetch(context.Background(), FetchRequest{
			ChannelKey:  env.leader.state.ChannelKey,
			Epoch:       env.leader.state.Epoch,
			ReplicaID:   follower.localNode,
			FetchOffset: follower.state.LEO,
			OffsetEpoch: follower.state.OffsetEpoch,
			MaxBytes:    1024,
		})
		if err != nil {
			t.Fatalf("Fetch() error = %v", err)
		}
		if err := follower.ApplyFetch(context.Background(), ApplyFetchRequest{
			ChannelKey: env.leader.state.ChannelKey,
			Epoch:      fetch.Epoch,
			Leader:     env.leader.localNode,
			TruncateTo: fetch.TruncateTo,
			Records:    fetch.Records,
			LeaderHW:   fetch.HW,
		}); err != nil {
			t.Fatalf("ApplyFetch() error = %v", err)
		}
		if err := env.leader.ApplyProgressAck(context.Background(), ProgressAckRequest{
			ChannelKey:  env.leader.state.ChannelKey,
			Epoch:       env.leader.state.Epoch,
			ReplicaID:   follower.localNode,
			MatchOffset: follower.state.LEO,
		}); err != nil {
			t.Fatalf("ApplyProgressAck() error = %v", err)
		}
	}

	replicateFollowerAndAck(env.follower2)
	if got := env.leader.progress[2]; got != 1 {
		t.Fatalf("progress[2] = %d", got)
	}
	if got := env.leader.state.HW; got != 0 {
		t.Fatalf("HW after follower2 ack = %d", got)
	}
	select {
	case <-done:
		t.Fatal("append returned before MinISR was satisfied")
	default:
	}

	replicateFollowerAndAck(env.follower3)
	if got := env.leader.progress[3]; got != 1 {
		t.Fatalf("progress[3] = %d", got)
	}
	if got := env.leader.state.HW; got != 1 {
		t.Fatalf("HW after follower3 ack = %d", got)
	}
	res := <-done
	if res.NextCommitHW != 1 {
		t.Fatalf("NextCommitHW = %d", res.NextCommitHW)
	}
}

func TestApplyProgressAckIgnoresStaleEpoch(t *testing.T) {
	env := newFetchEnvWithHistory(t)

	err := env.replica.ApplyProgressAck(context.Background(), ProgressAckRequest{
		ChannelKey:  "group-10",
		Epoch:       6,
		ReplicaID:   2,
		MatchOffset: 5,
	})
	if !errors.Is(err, ErrStaleMeta) {
		t.Fatalf("expected ErrStaleMeta, got %v", err)
	}
	if got := env.replica.progress[2]; got != 4 {
		t.Fatalf("progress[2] = %d, want 4", got)
	}
}

func TestApplyProgressAckPreservesMonotonicReplicaProgress(t *testing.T) {
	env := newFetchEnvWithHistory(t)

	if err := env.replica.ApplyProgressAck(context.Background(), ProgressAckRequest{
		ChannelKey:  "group-10",
		Epoch:       7,
		ReplicaID:   2,
		MatchOffset: 5,
	}); err != nil {
		t.Fatalf("ApplyProgressAck(first) error = %v", err)
	}
	if err := env.replica.ApplyProgressAck(context.Background(), ProgressAckRequest{
		ChannelKey:  "group-10",
		Epoch:       7,
		ReplicaID:   2,
		MatchOffset: 3,
	}); err != nil {
		t.Fatalf("ApplyProgressAck(second) error = %v", err)
	}
	if got := env.replica.progress[2]; got != 5 {
		t.Fatalf("progress[2] = %d, want 5", got)
	}
}
