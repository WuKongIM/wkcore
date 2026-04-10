package isr

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestFetchRejectsInvalidBudget(t *testing.T) {
	r := newLeaderReplica(t)

	_, err := r.Fetch(context.Background(), FetchRequest{
		GroupKey:    "group-10",
		Epoch:       7,
		ReplicaID:   2,
		FetchOffset: 0,
		OffsetEpoch: 3,
		MaxBytes:    0,
	})
	if !errors.Is(err, ErrInvalidFetchBudget) {
		t.Fatalf("expected ErrInvalidFetchBudget, got %v", err)
	}
}

func TestFetchRejectsMismatchedGroupKey(t *testing.T) {
	r := newLeaderReplica(t)

	_, err := r.Fetch(context.Background(), FetchRequest{
		GroupKey:    "group-other",
		Epoch:       7,
		ReplicaID:   2,
		FetchOffset: 0,
		OffsetEpoch: 3,
		MaxBytes:    1024,
	})
	if !errors.Is(err, ErrStaleMeta) {
		t.Fatalf("expected ErrStaleMeta, got %v", err)
	}
}

func TestFetchReturnsTruncateToWhenOffsetEpochDiverges(t *testing.T) {
	env := newFetchEnvWithHistory(t)

	result, err := env.replica.Fetch(context.Background(), FetchRequest{
		GroupKey:    "group-10",
		Epoch:       7,
		ReplicaID:   2,
		FetchOffset: 5,
		OffsetEpoch: 4,
		MaxBytes:    1024,
	})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if result.TruncateTo == nil || *result.TruncateTo != 4 {
		t.Fatalf("result.TruncateTo = %v", result.TruncateTo)
	}
}

func TestFetchReturnsSnapshotRequiredWhenFollowerFallsBehindLogStart(t *testing.T) {
	env := newFetchEnvWithHistory(t)
	env.replica.state.LogStartOffset = 4

	_, err := env.replica.Fetch(context.Background(), FetchRequest{
		GroupKey:    "group-10",
		Epoch:       7,
		ReplicaID:   2,
		FetchOffset: 3,
		OffsetEpoch: 3,
		MaxBytes:    1024,
	})
	if !errors.Is(err, ErrSnapshotRequired) {
		t.Fatalf("expected ErrSnapshotRequired, got %v", err)
	}
}

func TestFetchUpdatesLeaderProgressFromFollowerAck(t *testing.T) {
	env := newFetchEnvWithHistory(t)

	_, err := env.replica.Fetch(context.Background(), FetchRequest{
		GroupKey:    "group-10",
		Epoch:       7,
		ReplicaID:   2,
		FetchOffset: 5,
		OffsetEpoch: 7,
		MaxBytes:    1024,
	})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if got := env.replica.progress[2]; got != 5 {
		t.Fatalf("progress[2] = %d", got)
	}
}

func TestFetchDoesNotHoldReplicaLockAcrossLogRead(t *testing.T) {
	env := newFetchEnvWithHistory(t)
	env.log.readStarted = make(chan struct{}, 1)
	env.log.readContinue = make(chan struct{})

	done := make(chan error, 1)
	go func() {
		_, err := env.replica.Fetch(context.Background(), FetchRequest{
			GroupKey:    "group-10",
			Epoch:       7,
			ReplicaID:   2,
			FetchOffset: 0,
			OffsetEpoch: 3,
			MaxBytes:    1024,
		})
		done <- err
	}()

	<-env.log.readStarted

	statusReady := make(chan struct{}, 1)
	go func() {
		_ = env.replica.Status()
		statusReady <- struct{}{}
	}()

	select {
	case <-statusReady:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Status blocked while Fetch log.Read was in progress")
	}

	close(env.log.readContinue)
	if err := <-done; err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
}

func TestFetchDoesNotHoldReplicaLockAcrossCheckpointStore(t *testing.T) {
	env := newFetchEnvWithHistory(t)
	env.checkpoints.storeStarted = make(chan struct{}, 1)
	env.checkpoints.storeContinue = make(chan struct{})

	done := make(chan error, 1)
	go func() {
		_, err := env.replica.Fetch(context.Background(), FetchRequest{
			GroupKey:    "group-10",
			Epoch:       7,
			ReplicaID:   2,
			FetchOffset: 6,
			OffsetEpoch: 7,
			MaxBytes:    1024,
		})
		done <- err
	}()

	<-env.checkpoints.storeStarted

	statusReady := make(chan struct{}, 1)
	go func() {
		_ = env.replica.Status()
		statusReady <- struct{}{}
	}()

	select {
	case <-statusReady:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Status blocked while Fetch checkpoint.Store was in progress")
	}

	close(env.checkpoints.storeContinue)
	if err := <-done; err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
}
