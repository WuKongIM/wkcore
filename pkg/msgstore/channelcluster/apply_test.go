package channelcluster

import (
	"context"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/consensus/isr"
)

func TestCheckpointBridgeReplaysCommittedRecordsIntoIdempotencyState(t *testing.T) {
	store := &fakeStateStore{}
	key := ChannelKey{ChannelID: "c1", ChannelType: 1}
	log := &fakeMessageLog{
		records: []LogRecord{
			{
				Offset: 0,
				Payload: mustEncodeStoredMessage(t, storedMessage{
					MessageID:   11,
					SenderUID:   "u1",
					ClientMsgNo: "m1",
					PayloadHash: hashPayload([]byte("one")),
					Payload:     []byte("one"),
				}),
			},
			{
				Offset: 1,
				Payload: mustEncodeStoredMessage(t, storedMessage{
					MessageID:   12,
					SenderUID:   "u1",
					ClientMsgNo: "m2",
					PayloadHash: hashPayload([]byte("two")),
					Payload:     []byte("two"),
				}),
			},
		},
	}
	bridge, err := newCheckpointBridge(&memoryCheckpointStore{}, log, key, store, 8)
	if err != nil {
		t.Fatalf("newCheckpointBridge() error = %v", err)
	}

	if err := bridge.Store(isr.Checkpoint{Epoch: 3, HW: 2}); err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	entry, ok, err := store.GetIdempotency(IdempotencyKey{
		ChannelID:   "c1",
		ChannelType: 1,
		SenderUID:   "u1",
		ClientMsgNo: "m2",
	})
	if err != nil {
		t.Fatalf("GetIdempotency() error = %v", err)
	}
	if !ok {
		t.Fatal("expected idempotency entry")
	}
	if entry.MessageSeq != 2 {
		t.Fatalf("MessageSeq = %d, want 2", entry.MessageSeq)
	}
}

func TestSnapshotBridgeRestoresStateBeforeServingRecoveredReads(t *testing.T) {
	store := &fakeStateStore{}
	base := &recordingSnapshotApplier{}
	bridge := newSnapshotBridge(base, store)

	if err := bridge.InstallSnapshot(context.Background(), isr.Snapshot{
		GroupID:   8,
		Epoch:     4,
		EndOffset: 9,
		Payload:   []byte("snapshot"),
	}); err != nil {
		t.Fatalf("InstallSnapshot() error = %v", err)
	}
	if !base.called {
		t.Fatal("expected base snapshot applier to be called")
	}
	if !store.restoreCalled {
		t.Fatal("expected state restore to be called")
	}
}

type memoryCheckpointStore struct {
	checkpoint isr.Checkpoint
}

func (m *memoryCheckpointStore) Load() (isr.Checkpoint, error) {
	return m.checkpoint, nil
}

func (m *memoryCheckpointStore) Store(checkpoint isr.Checkpoint) error {
	m.checkpoint = checkpoint
	return nil
}

type recordingSnapshotApplier struct {
	called bool
}

func (r *recordingSnapshotApplier) InstallSnapshot(context.Context, isr.Snapshot) error {
	r.called = true
	return nil
}

func mustEncodeStoredMessage(t *testing.T, message storedMessage) []byte {
	t.Helper()
	payload, err := encodeStoredMessage(message)
	if err != nil {
		t.Fatalf("encodeStoredMessage() error = %v", err)
	}
	return payload
}
