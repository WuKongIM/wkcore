package isr

import (
	"context"
	"errors"
	"testing"
)

func TestFetchRejectsInvalidBudget(t *testing.T) {
	r := newLeaderReplica(t)

	_, err := r.Fetch(context.Background(), FetchRequest{
		GroupID:     10,
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

func TestFetchReturnsTruncateToWhenOffsetEpochDiverges(t *testing.T) {
	env := newFetchEnvWithHistory(t)

	result, err := env.replica.Fetch(context.Background(), FetchRequest{
		GroupID:     10,
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
		GroupID:     10,
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
		GroupID:     10,
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
