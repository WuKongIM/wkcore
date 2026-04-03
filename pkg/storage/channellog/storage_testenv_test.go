package channellog

import "testing"

func openTestDB(t *testing.T) *DB {
	t.Helper()

	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return db
}

func openTestStore(t *testing.T, key ChannelKey) *Store {
	t.Helper()
	return openTestDB(t).ForChannel(key)
}

func mustAppendPayloads(t *testing.T, store *Store, payloads []string) {
	t.Helper()

	raw := make([][]byte, 0, len(payloads))
	for _, payload := range payloads {
		raw = append(raw, []byte(payload))
	}
	if _, err := store.appendPayloads(raw); err != nil {
		t.Fatalf("appendPayloads() error = %v", err)
	}
}
