package channellog

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
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
	bridge, err := newCheckpointBridge(&memoryCheckpointStore{}, log, key, store, channelGroupKey(key))
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

func TestCheckpointBridgeUsesAtomicCheckpointCommitWhenSupported(t *testing.T) {
	key := ChannelKey{ChannelID: "c1", ChannelType: 1}
	state := &atomicCommitStateStore{}
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
		},
	}
	base := &failingCheckpointStore{err: errors.New("checkpoint should not be called")}

	bridge, err := newCheckpointBridge(base, log, key, state, channelGroupKey(key))
	if err != nil {
		t.Fatalf("newCheckpointBridge() error = %v", err)
	}

	checkpoint := isr.Checkpoint{Epoch: 3, HW: 1}
	if err := bridge.Store(checkpoint); err != nil {
		t.Fatalf("Store() error = %v", err)
	}
	if !state.called {
		t.Fatal("expected atomic state commit to be used")
	}
	if state.checkpoint != checkpoint {
		t.Fatalf("checkpoint = %+v, want %+v", state.checkpoint, checkpoint)
	}
	if len(state.batch) != 1 {
		t.Fatalf("len(batch) = %d, want 1", len(state.batch))
	}
	if base.called {
		t.Fatal("expected base checkpoint store to be bypassed")
	}
}

func TestSnapshotBridgeRestoresStateBeforeServingRecoveredReads(t *testing.T) {
	store := &fakeStateStore{}
	base := &recordingSnapshotApplier{}
	bridge := newSnapshotBridge(base, store)

	if err := bridge.InstallSnapshot(context.Background(), isr.Snapshot{
		GroupKey:  channelGroupKey(ChannelKey{ChannelID: "c1", ChannelType: 1}),
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

type failingCheckpointStore struct {
	called bool
	err    error
}

func (f *failingCheckpointStore) Load() (isr.Checkpoint, error) {
	return isr.Checkpoint{}, nil
}

func (f *failingCheckpointStore) Store(isr.Checkpoint) error {
	f.called = true
	return f.err
}

type atomicCommitStateStore struct {
	fakeStateStore
	called     bool
	checkpoint isr.Checkpoint
	batch      []appliedMessage
}

func (s *atomicCommitStateStore) CommitCommittedWithCheckpoint(checkpoint isr.Checkpoint, batch []appliedMessage) error {
	s.called = true
	s.checkpoint = checkpoint
	s.batch = append([]appliedMessage(nil), batch...)
	for _, msg := range batch {
		if err := s.PutIdempotency(msg.key, msg.entry); err != nil {
			return err
		}
	}
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
