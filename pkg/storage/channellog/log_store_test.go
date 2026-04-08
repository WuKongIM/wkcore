package channellog

import "testing"

func TestStoreAppendAndReadByOffset(t *testing.T) {
	store := openTestStore(t, ChannelKey{ChannelID: "c1", ChannelType: 1})

	base, err := store.appendPayloads([][]byte{[]byte("one"), []byte("two")})
	if err != nil {
		t.Fatalf("appendPayloads() error = %v", err)
	}
	if base != 0 {
		t.Fatalf("base = %d, want 0", base)
	}

	records, err := store.readOffsets(0, 2, 1024)
	if err != nil {
		t.Fatalf("readOffsets() error = %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("len(records) = %d, want 2", len(records))
	}
	if records[1].Offset != 1 {
		t.Fatalf("records[1].Offset = %d, want 1", records[1].Offset)
	}
}

func TestDBReadScopesByGroupKeyAndBudget(t *testing.T) {
	db := openTestDB(t)
	first := db.ForChannel(ChannelKey{ChannelID: "c1", ChannelType: 1})
	second := db.ForChannel(ChannelKey{ChannelID: "c2", ChannelType: 1})
	mustAppendPayloads(t, first, []string{"one", "two"})
	mustAppendPayloads(t, second, []string{"zzz"})

	records, err := db.Read(channelGroupKey(first.key), 0, 10, len("one"))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}
}

func TestDBReadBudgetCountsPayloadBytesOnly(t *testing.T) {
	db := openTestDB(t)
	store := db.ForChannel(ChannelKey{ChannelID: "c1", ChannelType: 1})
	mustAppendPayloads(t, store, []string{"one", "two"})

	records, err := db.Read(channelGroupKey(store.key), 0, 10, len("one")+len("two"))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("len(records) = %d, want 2", len(records))
	}
}

func TestStoreCachedLEOTracksAppendAndTruncate(t *testing.T) {
	store := openTestStore(t, ChannelKey{ChannelID: "c1", ChannelType: 1})

	base, err := store.appendPayloads([][]byte{[]byte("one"), []byte("two")})
	if err != nil {
		t.Fatalf("appendPayloads() error = %v", err)
	}
	if base != 0 {
		t.Fatalf("base = %d, want 0", base)
	}
	if !store.leoLoaded {
		t.Fatal("expected LEO cache to be loaded after append")
	}
	if store.cachedLEO != 2 {
		t.Fatalf("cachedLEO = %d, want 2", store.cachedLEO)
	}

	if err := store.truncateOffsets(1); err != nil {
		t.Fatalf("truncateOffsets() error = %v", err)
	}
	if !store.leoLoaded {
		t.Fatal("expected LEO cache to stay loaded after truncate")
	}
	if store.cachedLEO != 1 {
		t.Fatalf("cachedLEO after truncate = %d, want 1", store.cachedLEO)
	}

	base, err = store.appendPayloads([][]byte{[]byte("three")})
	if err != nil {
		t.Fatalf("appendPayloads(after truncate) error = %v", err)
	}
	if base != 1 {
		t.Fatalf("base after truncate = %d, want 1", base)
	}
	if store.cachedLEO != 2 {
		t.Fatalf("cachedLEO after reappend = %d, want 2", store.cachedLEO)
	}
}

func TestStoreSyncAvoidsFlushAndPreservesTrimmedLogAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	key := ChannelKey{ChannelID: "c1", ChannelType: 1}
	store := db.ForChannel(key)

	if _, err := store.appendPayloads([][]byte{[]byte("one"), []byte("two")}); err != nil {
		t.Fatalf("appendPayloads() error = %v", err)
	}
	if err := store.truncateOffsets(1); err != nil {
		t.Fatalf("truncateOffsets() error = %v", err)
	}

	beforeFlush := db.db.Metrics().Flush.Count
	if err := store.sync(); err != nil {
		t.Fatalf("sync() error = %v", err)
	}
	afterFlush := db.db.Metrics().Flush.Count
	if afterFlush != beforeFlush {
		t.Fatalf("Flush.Count changed from %d to %d", beforeFlush, afterFlush)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(reopen) error = %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("Close(reopen) error = %v", err)
		}
	}()

	reloadedStore := reopened.ForChannel(key)
	leo, err := reloadedStore.leo()
	if err != nil {
		t.Fatalf("leo() error = %v", err)
	}
	if leo != 1 {
		t.Fatalf("leo = %d, want 1", leo)
	}

	records, err := reloadedStore.readOffsets(0, 10, 1024)
	if err != nil {
		t.Fatalf("readOffsets() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}
	if string(records[0].Payload) != "one" {
		t.Fatalf("records[0].Payload = %q, want %q", records[0].Payload, "one")
	}
}
