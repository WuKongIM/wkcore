package store

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

func newTestChannelStore(tb testing.TB) *ChannelStore {
	tb.Helper()
	return openTestChannelStore(tb, channel.ChannelKey("channel/1/c1"), channel.ChannelID{ID: "c1", Type: 1})
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
