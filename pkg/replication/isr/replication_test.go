package isr

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestApplyFetchTruncatesUncommittedTailBeforeAppending(t *testing.T) {
	env := newFollowerEnv(t)
	env.replica.state.HW = 4
	env.replica.state.LEO = 6
	env.log.leo = 6
	env.log.records = []Record{
		{Payload: []byte("a"), SizeBytes: 1},
		{Payload: []byte("b"), SizeBytes: 1},
		{Payload: []byte("c"), SizeBytes: 1},
		{Payload: []byte("d"), SizeBytes: 1},
		{Payload: []byte("e"), SizeBytes: 1},
		{Payload: []byte("f"), SizeBytes: 1},
	}
	truncateTo := uint64(4)

	err := env.replica.ApplyFetch(context.Background(), ApplyFetchRequest{
		GroupKey:   "group-10",
		Epoch:      7,
		Leader:     1,
		TruncateTo: &truncateTo,
		Records:    []Record{{Payload: []byte("x"), SizeBytes: 1}},
		LeaderHW:   5,
	})
	if err != nil {
		t.Fatalf("ApplyFetch() error = %v", err)
	}
	if got := env.log.truncateCalls; !reflect.DeepEqual(got, []uint64{4}) {
		t.Fatalf("truncate calls = %v", got)
	}
}

func TestApplyFetchRejectsMismatchedGroupKey(t *testing.T) {
	env := newFollowerEnv(t)

	err := env.replica.ApplyFetch(context.Background(), ApplyFetchRequest{
		GroupKey: "group-other",
		Epoch:    7,
		Leader:   1,
		LeaderHW: 0,
	})
	if !errors.Is(err, ErrStaleMeta) {
		t.Fatalf("expected ErrStaleMeta, got %v", err)
	}
}

func TestApplyFetchAdvancesCheckpointToMinLeaderHWAndLEO(t *testing.T) {
	env := newFollowerEnv(t)

	err := env.replica.ApplyFetch(context.Background(), ApplyFetchRequest{
		GroupKey: "group-10",
		Epoch:    7,
		Leader:   1,
		Records:  []Record{{Payload: []byte("a"), SizeBytes: 1}},
		LeaderHW: 10,
	})
	if err != nil {
		t.Fatalf("ApplyFetch() error = %v", err)
	}
	if got := env.checkpoints.lastStored(); got.HW != 1 {
		t.Fatalf("stored checkpoint = %+v", got)
	}
}

func TestApplyFetchSkipsCheckpointWriteWhenHWDoesNotAdvance(t *testing.T) {
	env := newFollowerEnv(t)

	err := env.replica.ApplyFetch(context.Background(), ApplyFetchRequest{
		GroupKey: "group-10",
		Epoch:    7,
		Leader:   1,
		Records:  []Record{{Payload: []byte("a"), SizeBytes: 1}},
		LeaderHW: 0,
	})
	if err != nil {
		t.Fatalf("ApplyFetch() error = %v", err)
	}
	if got := len(env.checkpoints.stored); got != 0 {
		t.Fatalf("checkpoint writes = %d, want 0", got)
	}
	if got := env.replica.state.HW; got != 0 {
		t.Fatalf("HW = %d, want 0", got)
	}
	if got := env.replica.state.LEO; got != 1 {
		t.Fatalf("LEO = %d, want 1", got)
	}
}

func TestApplyFetchRejectsStaleEpoch(t *testing.T) {
	env := newFollowerEnv(t)

	err := env.replica.ApplyFetch(context.Background(), ApplyFetchRequest{
		GroupKey: "group-10",
		Epoch:    6,
		Leader:   1,
		LeaderHW: 0,
	})
	if !errors.Is(err, ErrStaleMeta) {
		t.Fatalf("expected ErrStaleMeta, got %v", err)
	}
}

func TestApplyFetchRejectsTruncateBelowHW(t *testing.T) {
	env := newFollowerEnv(t)
	env.replica.state.HW = 4
	truncateTo := uint64(3)

	err := env.replica.ApplyFetch(context.Background(), ApplyFetchRequest{
		GroupKey:   "group-10",
		Epoch:      7,
		Leader:     1,
		TruncateTo: &truncateTo,
		LeaderHW:   4,
	})
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("expected ErrCorruptState, got %v", err)
	}
}
