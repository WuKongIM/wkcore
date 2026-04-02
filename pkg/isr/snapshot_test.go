package isr

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestInstallSnapshotPersistsPayloadBeforeCheckpoint(t *testing.T) {
	env := newFollowerEnv(t)
	env.log.leo = 8
	snap := Snapshot{
		GroupID:   10,
		Epoch:     7,
		EndOffset: 8,
		Payload:   []byte("snap"),
	}

	if err := env.replica.InstallSnapshot(context.Background(), snap); err != nil {
		t.Fatalf("InstallSnapshot() error = %v", err)
	}
	if got := env.calls.snapshot(); !reflect.DeepEqual(got, []string{
		"snapshot.install:8",
		"checkpoint.store:8",
	}) {
		t.Fatalf("call order = %v", got)
	}
}

func TestInstallSnapshotRejectsLogStoreBehindSnapshotEndOffset(t *testing.T) {
	env := newFollowerEnv(t)
	env.log.leo = 5

	err := env.replica.InstallSnapshot(context.Background(), Snapshot{
		GroupID:   10,
		Epoch:     7,
		EndOffset: 8,
	})
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("expected ErrCorruptState, got %v", err)
	}
}

func TestNewReplicaRecoversInstalledSnapshotOffsets(t *testing.T) {
	env := newFollowerEnv(t)
	env.log.leo = 8
	snap := Snapshot{
		GroupID:   10,
		Epoch:     7,
		EndOffset: 8,
		Payload:   []byte("snap"),
	}

	if err := env.replica.InstallSnapshot(context.Background(), snap); err != nil {
		t.Fatalf("InstallSnapshot() error = %v", err)
	}

	reloaded := newReplicaFromEnv(t, env)
	st := reloaded.Status()
	if st.LogStartOffset != 8 || st.HW != 8 || st.LEO != 8 {
		t.Fatalf("status = %+v", st)
	}
}
