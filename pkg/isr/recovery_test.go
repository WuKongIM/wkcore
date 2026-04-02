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
