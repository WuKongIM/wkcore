package isr

import (
	"errors"
	"reflect"
	"testing"
)

func TestNewReplicaTruncatesDirtyTailToCheckpointHW(t *testing.T) {
	env := newTestEnv(t)
	env.log.leo = 5
	env.checkpoints.loadErr = nil
	env.checkpoints.checkpoint = Checkpoint{
		Epoch:          3,
		LogStartOffset: 0,
		HW:             3,
	}

	r := newReplicaFromEnv(t, env)
	st := r.Status()
	if st.LEO != 3 || st.HW != 3 {
		t.Fatalf("status = %+v", st)
	}
	if got := env.log.truncateCalls; !reflect.DeepEqual(got, []uint64{3}) {
		t.Fatalf("truncate calls = %v", got)
	}
}

func TestNewReplicaRejectsCheckpointBeyondLEO(t *testing.T) {
	env := newTestEnv(t)
	env.log.leo = 2
	env.checkpoints.loadErr = nil
	env.checkpoints.checkpoint = Checkpoint{
		LogStartOffset: 0,
		HW:             3,
	}

	_, err := NewReplica(env.config())
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("expected ErrCorruptState, got %v", err)
	}
}

func TestNewReplicaBootstrapsEmptyState(t *testing.T) {
	env := newTestEnv(t)

	r := newReplicaFromEnv(t, env)
	st := r.Status()
	if st.LogStartOffset != 0 || st.HW != 0 || st.LEO != 0 {
		t.Fatalf("status = %+v", st)
	}
}

func TestBecomeLeaderTruncatesThenSyncsThenAppendsEpochPoint(t *testing.T) {
	env := newRecoveredLeaderEnv(t)
	env.log.leo = 6
	env.replica.mustApplyMeta(t, activeMeta(7, 1))

	if err := env.replica.BecomeLeader(activeMeta(7, 1)); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	if got := env.calls.snapshot(); !reflect.DeepEqual(got, []string{
		"log.truncate:4",
		"log.sync",
		"history.truncate:4",
		"history.append:7@4",
	}) {
		t.Fatalf("call order = %v", got)
	}
}

func TestBecomeLeaderReplaysIdenticalEpochPointIdempotently(t *testing.T) {
	env := newRecoveredLeaderEnv(t)
	env.history.points = []EpochPoint{{Epoch: 7, StartOffset: 4}}
	env.replica.mustApplyMeta(t, activeMeta(7, 1))

	if err := env.replica.BecomeLeader(activeMeta(7, 1)); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	if env.replica.state.Role != RoleLeader {
		t.Fatalf("role = %v", env.replica.state.Role)
	}
	if got := len(env.replica.progress); got != 3 {
		t.Fatalf("progress size = %d", got)
	}
	if got := env.replica.progress[1]; got != 4 {
		t.Fatalf("leader progress = %d", got)
	}
}

func TestBecomeLeaderReusesRecoveredEpochPointWhenLeaderRestartsSameEpoch(t *testing.T) {
	env := newTestEnv(t)
	env.log.leo = 1
	env.checkpoints.loadErr = nil
	env.checkpoints.checkpoint = Checkpoint{
		Epoch:          7,
		LogStartOffset: 0,
		HW:             1,
	}
	env.history.loadErr = nil
	env.history.points = []EpochPoint{{Epoch: 7, StartOffset: 0}}
	env.replica = newReplicaFromEnv(t, env)
	env.replica.mustApplyMeta(t, activeMeta(7, 1))

	if err := env.replica.BecomeLeader(activeMeta(7, 1)); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	if got := env.history.appended; len(got) != 0 {
		t.Fatalf("unexpected appended history = %+v", got)
	}
	if got := env.replica.state.Role; got != RoleLeader {
		t.Fatalf("role = %v", got)
	}
	if got := env.replica.state.LEO; got != 1 {
		t.Fatalf("LEO = %d", got)
	}
	if got := env.replica.progress[1]; got != 1 {
		t.Fatalf("leader progress = %d", got)
	}
}
