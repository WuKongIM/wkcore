package store

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

func TestChannelStoreAppendAndRead(t *testing.T) {
	st := newTestChannelStore(t)

	base, err := st.Append([]channel.Record{
		{Payload: []byte("one"), SizeBytes: 3},
		{Payload: []byte("two"), SizeBytes: 3},
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if base != 0 {
		t.Fatalf("base = %d, want 0", base)
	}

	records, err := st.Read(0, 1024)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("len(records) = %d, want 2", len(records))
	}
	if string(records[1].Payload) != "two" {
		t.Fatalf("records[1].Payload = %q, want %q", records[1].Payload, "two")
	}
}

func TestEngineReadScopesByChannelKeyAndBudget(t *testing.T) {
	engine := openTestEngine(t)
	first := engine.ForChannel(channel.ChannelKey("channel/1/c1"), channel.ChannelID{ID: "c1", Type: 1})
	second := engine.ForChannel(channel.ChannelKey("channel/1/c2"), channel.ChannelID{ID: "c2", Type: 1})
	mustAppendRecords(t, first, []string{"one", "two"})
	mustAppendRecords(t, second, []string{"zzz"})

	records, err := engine.Read(first.key, 0, 10, len("one"))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}
}

func TestChannelStoreLEOTracksAppendAndTruncate(t *testing.T) {
	st := newTestChannelStore(t)

	if _, err := st.Append([]channel.Record{
		{Payload: []byte("one"), SizeBytes: 3},
		{Payload: []byte("two"), SizeBytes: 3},
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if got := st.LEO(); got != 2 {
		t.Fatalf("LEO() = %d, want 2", got)
	}

	if err := st.Truncate(1); err != nil {
		t.Fatalf("Truncate() error = %v", err)
	}
	if got := st.LEO(); got != 1 {
		t.Fatalf("LEO() after truncate = %d, want 1", got)
	}

	base, err := st.Append([]channel.Record{{Payload: []byte("three"), SizeBytes: 5}})
	if err != nil {
		t.Fatalf("Append(after truncate) error = %v", err)
	}
	if base != 1 {
		t.Fatalf("base after truncate = %d, want 1", base)
	}
	if got := st.LEO(); got != 2 {
		t.Fatalf("LEO() after re-append = %d, want 2", got)
	}
}

func TestChannelStoreSyncPreservesTrimmedLogAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	key := channel.ChannelKey("channel/1/c1")
	id := channel.ChannelID{ID: "c1", Type: 1}
	st := engine.ForChannel(key, id)

	mustAppendRecords(t, st, []string{"one", "two"})
	if err := st.Truncate(1); err != nil {
		t.Fatalf("Truncate() error = %v", err)
	}
	if err := st.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(reopen) error = %v", err)
	}
	defer reopened.Close()

	reloaded := reopened.ForChannel(key, id)
	if got := reloaded.LEO(); got != 1 {
		t.Fatalf("LEO() = %d, want 1", got)
	}
	records, err := reloaded.Read(0, 1024)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("len(records) = %d, want 1", len(records))
	}
	if string(records[0].Payload) != "one" {
		t.Fatalf("records[0].Payload = %q, want %q", records[0].Payload, "one")
	}
}
