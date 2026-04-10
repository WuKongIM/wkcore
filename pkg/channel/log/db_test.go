package log

import (
	"path/filepath"
	"testing"
)

func TestOpenCreatesDBAndForChannelBindsDerivedChannelKey(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	key := ChannelKey{ChannelID: "c1", ChannelType: 1}
	store := db.ForChannel(key)
	if store == nil {
		t.Fatal("expected Store")
	}
	if store.key != key {
		t.Fatalf("Store.key = %+v, want %+v", store.key, key)
	}
	if store.channelKey != isrChannelKeyForChannel(key) {
		t.Fatalf("Store.channelKey = %q, want %q", store.channelKey, isrChannelKeyForChannel(key))
	}
}

func TestDBForChannelReturnsStableStorePerKey(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	first := db.ForChannel(ChannelKey{ChannelID: "c1", ChannelType: 1})
	second := db.ForChannel(ChannelKey{ChannelID: "c1", ChannelType: 1})
	other := db.ForChannel(ChannelKey{ChannelID: "c2", ChannelType: 1})

	if first != second {
		t.Fatal("expected ForChannel() to reuse the same Store for the same key")
	}
	if first == other {
		t.Fatal("expected different keys to have different Store instances")
	}
}

func TestDBImplementsMessageLogSurface(t *testing.T) {
	var _ MessageLog = (*DB)(nil)
}
