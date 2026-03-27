package raftstore

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/WuKongIM/wraft/multiraft"
	"go.etcd.io/raft/v3/raftpb"
)

func TestPebbleOpenForGroupReturnsStorage(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	if db.ForGroup(7) == nil {
		t.Fatal("ForGroup(7) returned nil storage")
	}
}

func TestPebbleEntryKeysSortByGroupAndIndex(t *testing.T) {
	a := encodeEntryKey(7, 5)
	b := encodeEntryKey(7, 6)
	c := encodeEntryKey(8, 1)

	if bytes.Compare(a, b) >= 0 {
		t.Fatalf("entry keys for one group did not sort by index")
	}
	if bytes.Compare(b, c) >= 0 {
		t.Fatalf("entry keys did not sort by group before index")
	}
}

func TestPebbleStateRoundTripAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	store := db.ForGroup(9)

	hs := raftpb.HardState{Term: 2, Vote: 1, Commit: 7}
	snap := raftpb.Snapshot{
		Data: []byte("snap"),
		Metadata: raftpb.SnapshotMetadata{
			Index: 7,
			Term:  2,
			ConfState: raftpb.ConfState{
				Voters: []uint64{1, 2},
			},
		},
	}
	if err := store.Save(context.Background(), multiraft.PersistentState{
		HardState: &hs,
		Snapshot:  &snap,
	}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	if err := store.MarkApplied(context.Background(), 7); err != nil {
		t.Fatalf("MarkApplied() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	state, err := reopened.ForGroup(9).InitialState(context.Background())
	if err != nil {
		t.Fatalf("InitialState() error = %v", err)
	}
	if !reflect.DeepEqual(state.HardState, hs) {
		t.Fatalf("HardState = %#v, want %#v", state.HardState, hs)
	}
	if state.AppliedIndex != 7 {
		t.Fatalf("AppliedIndex = %d, want 7", state.AppliedIndex)
	}
	if !reflect.DeepEqual(state.ConfState, snap.Metadata.ConfState) {
		t.Fatalf("ConfState = %#v, want %#v", state.ConfState, snap.Metadata.ConfState)
	}
}

func TestPebbleSnapshotRoundTrip(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	store := db.ForGroup(9)
	snap := raftpb.Snapshot{
		Data: []byte("snap"),
		Metadata: raftpb.SnapshotMetadata{
			Index: 7,
			Term:  2,
			ConfState: raftpb.ConfState{
				Voters: []uint64{1, 2},
			},
		},
	}
	if err := store.Save(context.Background(), multiraft.PersistentState{
		Snapshot: &snap,
	}); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	got, err := store.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if !reflect.DeepEqual(got, snap) {
		t.Fatalf("Snapshot() = %#v, want %#v", got, snap)
	}
}

func TestPebbleGroupsAreIndependent(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "raft"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	left := db.ForGroup(9)
	right := db.ForGroup(10)

	hs := raftpb.HardState{Term: 2, Vote: 1, Commit: 7}
	if err := left.Save(context.Background(), multiraft.PersistentState{
		HardState: &hs,
	}); err != nil {
		t.Fatalf("left.Save() error = %v", err)
	}
	if err := left.MarkApplied(context.Background(), 7); err != nil {
		t.Fatalf("left.MarkApplied() error = %v", err)
	}

	leftState, err := left.InitialState(context.Background())
	if err != nil {
		t.Fatalf("left.InitialState() error = %v", err)
	}
	if !reflect.DeepEqual(leftState.HardState, hs) {
		t.Fatalf("left HardState = %#v, want %#v", leftState.HardState, hs)
	}
	if leftState.AppliedIndex != 7 {
		t.Fatalf("left AppliedIndex = %d, want 7", leftState.AppliedIndex)
	}

	rightState, err := right.InitialState(context.Background())
	if err != nil {
		t.Fatalf("right.InitialState() error = %v", err)
	}
	if !reflect.DeepEqual(rightState.HardState, raftpb.HardState{}) {
		t.Fatalf("right HardState = %#v, want zero value", rightState.HardState)
	}
	if rightState.AppliedIndex != 0 {
		t.Fatalf("right AppliedIndex = %d, want 0", rightState.AppliedIndex)
	}
}
