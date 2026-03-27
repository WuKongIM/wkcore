package raftstore

import (
	"bytes"
	"path/filepath"
	"testing"
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
