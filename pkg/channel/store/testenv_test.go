package store

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

func newTestChannelStore(tb testing.TB) *ChannelStore {
	tb.Helper()
	key, id := testChannelStoreIdentity("c1")
	return openTestChannelStore(tb, key, id)
}

func openTestEngine(tb testing.TB) *Engine {
	tb.Helper()

	engine, err := Open(tb.TempDir())
	if err != nil {
		tb.Fatalf("Open() error = %v", err)
	}
	tb.Cleanup(func() {
		if err := engine.Close(); err != nil {
			tb.Fatalf("Close() error = %v", err)
		}
	})
	return engine
}

func openTestChannelStore(tb testing.TB, key channel.ChannelKey, id channel.ChannelID) *ChannelStore {
	tb.Helper()
	return openTestEngine(tb).ForChannel(key, id)
}

func openTestChannelStoresOnEngine(tb testing.TB, engine *Engine, names ...string) []*ChannelStore {
	tb.Helper()

	stores := make([]*ChannelStore, 0, len(names))
	for _, name := range names {
		stores = append(stores, engine.ForChannel(testChannelStoreIdentity(name)))
	}
	return stores
}

func testChannelStoreIdentity(name string) (channel.ChannelKey, channel.ChannelID) {
	return channel.ChannelKey("channel/1/" + name), channel.ChannelID{ID: name, Type: 1}
}

func mustAppendRecords(t *testing.T, st *ChannelStore, payloads []string) {
	t.Helper()

	records := make([]channel.Record, 0, len(payloads))
	for _, payload := range payloads {
		records = append(records, channel.Record{
			Payload:   []byte(payload),
			SizeBytes: len(payload),
		})
	}
	if _, err := st.Append(records); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
}
