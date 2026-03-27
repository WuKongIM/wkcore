package wkdbraft

import (
	"context"
	"reflect"
	"testing"

	"github.com/WuKongIM/wraft/wkdb"
	"go.etcd.io/raft/v3/raftpb"
)

func TestWKDBRaftStorageSaveAndLoadRoundTrip(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	store := wkdb.NewRaftStorage(db, 13)

	hs := raftpb.HardState{Term: 2, Vote: 1, Commit: 7}
	entries := []raftpb.Entry{
		{Index: 6, Term: 2, Data: []byte("a")},
		{Index: 7, Term: 2, Data: []byte("b")},
	}
	snap := raftpb.Snapshot{
		Data: []byte("snap"),
		Metadata: raftpb.SnapshotMetadata{
			Index: 5,
			Term:  1,
			ConfState: raftpb.ConfState{
				Voters: []uint64{1, 2},
			},
		},
	}

	if err := store.Save(ctx, wkdb.RaftPersistentState{
		HardState: &hs,
		Entries:   entries,
		Snapshot:  &snap,
	}); err != nil {
		t.Fatalf("Save(): %v", err)
	}

	reopened := wkdb.NewRaftStorage(db, 13)
	state, err := reopened.InitialState(ctx)
	if err != nil {
		t.Fatalf("InitialState(): %v", err)
	}
	if !reflect.DeepEqual(state.HardState, hs) {
		t.Fatalf("HardState = %#v, want %#v", state.HardState, hs)
	}
	if !reflect.DeepEqual(state.ConfState, snap.Metadata.ConfState) {
		t.Fatalf("ConfState = %#v, want %#v", state.ConfState, snap.Metadata.ConfState)
	}

	gotEntries, err := reopened.Entries(ctx, 6, 8, 0)
	if err != nil {
		t.Fatalf("Entries(): %v", err)
	}
	if !reflect.DeepEqual(gotEntries, entries) {
		t.Fatalf("Entries = %#v, want %#v", gotEntries, entries)
	}

	gotSnap, err := reopened.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot(): %v", err)
	}
	if !reflect.DeepEqual(gotSnap, snap) {
		t.Fatalf("Snapshot = %#v, want %#v", gotSnap, snap)
	}
}

func TestWKDBRaftStorageMarkAppliedPersists(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	store := wkdb.NewRaftStorage(db, 13)

	if err := store.MarkApplied(ctx, 9); err != nil {
		t.Fatalf("MarkApplied(): %v", err)
	}

	state, err := wkdb.NewRaftStorage(db, 13).InitialState(ctx)
	if err != nil {
		t.Fatalf("InitialState(): %v", err)
	}
	if state.AppliedIndex != 9 {
		t.Fatalf("AppliedIndex = %d, want 9", state.AppliedIndex)
	}
}

func TestWKDBRaftStorageEntriesWindowing(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	store := wkdb.NewRaftStorage(db, 13)

	entries := []raftpb.Entry{
		{Index: 5, Term: 1, Data: []byte("a")},
		{Index: 6, Term: 2, Data: []byte("b")},
		{Index: 7, Term: 2, Data: []byte("c")},
	}
	if err := store.Save(ctx, wkdb.RaftPersistentState{Entries: entries}); err != nil {
		t.Fatalf("Save(): %v", err)
	}

	got, err := store.Entries(ctx, 6, 8, 0)
	if err != nil {
		t.Fatalf("Entries(): %v", err)
	}
	want := entries[1:]
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Entries(6,8) = %#v, want %#v", got, want)
	}

	first, err := store.FirstIndex(ctx)
	if err != nil {
		t.Fatalf("FirstIndex(): %v", err)
	}
	if first != 5 {
		t.Fatalf("FirstIndex = %d, want 5", first)
	}

	last, err := store.LastIndex(ctx)
	if err != nil {
		t.Fatalf("LastIndex(): %v", err)
	}
	if last != 7 {
		t.Fatalf("LastIndex = %d, want 7", last)
	}

	term, err := store.Term(ctx, 6)
	if err != nil {
		t.Fatalf("Term(): %v", err)
	}
	if term != 2 {
		t.Fatalf("Term(6) = %d, want 2", term)
	}
}
