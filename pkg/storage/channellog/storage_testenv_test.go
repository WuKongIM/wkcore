package channellog

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

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

func mustAppendStoredMessages(t *testing.T, store *Store, payloads ...string) {
	t.Helper()

	raw := make([][]byte, 0, len(payloads))
	for i, payload := range payloads {
		encoded, err := encodeStoredMessage(storedMessage{
			MessageID:   uint64(i + 1),
			SenderUID:   "u1",
			ClientMsgNo: "m" + string(rune('1'+i)),
			PayloadHash: hashPayload([]byte(payload)),
			Payload:     []byte(payload),
		})
		if err != nil {
			t.Fatalf("encodeStoredMessage() error = %v", err)
		}
		raw = append(raw, encoded)
	}
	if _, err := store.appendPayloads(raw); err != nil {
		t.Fatalf("appendPayloads() error = %v", err)
	}
}

func mustStoreCheckpoint(t *testing.T, store *Store, checkpoint isr.Checkpoint) {
	t.Helper()
	if err := store.storeCheckpoint(checkpoint); err != nil {
		t.Fatalf("storeCheckpoint() error = %v", err)
	}
}
