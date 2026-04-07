package channellog

import (
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

func openTestDB(tb testing.TB) *DB {
	tb.Helper()

	db, err := Open(tb.TempDir())
	if err != nil {
		tb.Fatalf("Open() error = %v", err)
	}
	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Fatalf("Close() error = %v", err)
		}
	})
	return db
}

func openTestStore(tb testing.TB, key ChannelKey) *Store {
	tb.Helper()
	return openTestDB(tb).ForChannel(key)
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
			FromUID:     "u1",
			ClientMsgNo: fmt.Sprintf("m%d", i+1),
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

func mustAppendMessages(t *testing.T, store *Store, messages ...Message) {
	t.Helper()

	raw := make([][]byte, 0, len(messages))
	for _, message := range messages {
		encoded, err := encodeMessage(message)
		if err != nil {
			t.Fatalf("encodeMessage() error = %v", err)
		}
		raw = append(raw, encoded)
	}
	if _, err := store.appendPayloads(raw); err != nil {
		t.Fatalf("appendPayloads() error = %v", err)
	}
}

func mustEncodeDurableMessage(t testing.TB, message Message) []byte {
	t.Helper()

	payload, err := encodeMessage(message)
	if err != nil {
		t.Fatalf("encodeMessage() error = %v", err)
	}
	return payload
}

func mustStoreCheckpoint(t *testing.T, store *Store, checkpoint isr.Checkpoint) {
	t.Helper()
	if err := store.storeCheckpoint(checkpoint); err != nil {
		t.Fatalf("storeCheckpoint() error = %v", err)
	}
}
