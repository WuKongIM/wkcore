package isr

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
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

func TestApplyFetchUsesAtomicStoreOnAppendOnlyHotPath(t *testing.T) {
	env := newTestEnv(t)
	env.localNode = 2
	env.applyFetch = &fakeApplyFetchStore{}
	env.checkpoints.loadErr = nil
	env.checkpoints.checkpoint = Checkpoint{
		Epoch:          7,
		LogStartOffset: 0,
		HW:             0,
	}
	env.history.loadErr = nil
	env.history.points = []EpochPoint{{Epoch: 7, StartOffset: 0}}
	env.replica = newReplicaFromEnvWithApplyFetchStore(t, env)
	env.replica.mustApplyMeta(t, activeMeta(7, 1))
	if err := env.replica.BecomeFollower(activeMeta(7, 1)); err != nil {
		t.Fatalf("BecomeFollower() error = %v", err)
	}

	err := env.replica.ApplyFetch(context.Background(), ApplyFetchRequest{
		GroupKey: "group-10",
		Epoch:    7,
		Leader:   1,
		Records:  []Record{{Payload: []byte("a"), SizeBytes: 1}},
		LeaderHW: 1,
	})
	if err != nil {
		t.Fatalf("ApplyFetch() error = %v", err)
	}
	if env.applyFetch.calls != 1 {
		t.Fatalf("apply fetch store calls = %d, want 1", env.applyFetch.calls)
	}
	if env.log.appendCount != 0 {
		t.Fatalf("log appendCount = %d, want 0", env.log.appendCount)
	}
	if env.log.syncCount != 0 {
		t.Fatalf("log syncCount = %d, want 0", env.log.syncCount)
	}
	if got := len(env.checkpoints.stored); got != 0 {
		t.Fatalf("checkpoint writes = %d, want 0", got)
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

func TestApplyFetchDoesNotHoldReplicaLockAcrossLogSync(t *testing.T) {
	env := newFollowerEnv(t)
	env.log.syncStarted = make(chan struct{}, 1)
	env.log.syncContinue = make(chan struct{})

	done := make(chan error, 1)
	go func() {
		done <- env.replica.ApplyFetch(context.Background(), ApplyFetchRequest{
			GroupKey: "group-10",
			Epoch:    7,
			Leader:   1,
			Records:  []Record{{Payload: []byte("a"), SizeBytes: 1}},
			LeaderHW: 1,
		})
	}()

	<-env.log.syncStarted

	statusReady := make(chan struct{}, 1)
	go func() {
		_ = env.replica.Status()
		statusReady <- struct{}{}
	}()

	select {
	case <-statusReady:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Status blocked while ApplyFetch log.Sync was in progress")
	}

	close(env.log.syncContinue)
	if err := <-done; err != nil {
		t.Fatalf("ApplyFetch() error = %v", err)
	}
}
