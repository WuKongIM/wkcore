package channellog

import (
	"errors"
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

func TestLoadMsgReturnsCommittedMessageOnly(t *testing.T) {
	store := openTestStore(t, ChannelKey{ChannelID: "c1", ChannelType: 1})
	mustAppendStoredMessages(t, store, "one", "two", "three")
	mustStoreCheckpoint(t, store, isr.Checkpoint{Epoch: 1, HW: 2})

	msg, err := store.LoadMsg(2)
	if err != nil {
		t.Fatalf("LoadMsg(2) error = %v", err)
	}
	if string(msg.Payload) != "two" {
		t.Fatalf("Payload = %q, want %q", msg.Payload, "two")
	}

	_, err = store.LoadMsg(3)
	if !errors.Is(err, ErrMessageNotFound) {
		t.Fatalf("expected ErrMessageNotFound, got %v", err)
	}
}

func TestLoadNextRangeMsgsCapsByCommittedHW(t *testing.T) {
	store := openTestStore(t, ChannelKey{ChannelID: "c1", ChannelType: 1})
	mustAppendStoredMessages(t, store, "one", "two", "three")
	mustStoreCheckpoint(t, store, isr.Checkpoint{Epoch: 1, HW: 2})

	msgs, err := store.LoadNextRangeMsgs(1, 0, 10)
	if err != nil {
		t.Fatalf("LoadNextRangeMsgs() error = %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("len(msgs) = %d, want 2", len(msgs))
	}
	if msgs[0].MessageSeq != 1 || msgs[1].MessageSeq != 2 {
		t.Fatalf("seqs = %+v", msgs)
	}
}

func TestLoadNextRangeMsgsTreatsZeroLimitAsUnlimited(t *testing.T) {
	store := openTestStore(t, ChannelKey{ChannelID: "c1", ChannelType: 1})
	mustAppendStoredMessages(t, store, "one", "two", "three")
	mustStoreCheckpoint(t, store, isr.Checkpoint{Epoch: 1, HW: 3})

	msgs, err := store.LoadNextRangeMsgs(1, 0, 0)
	if err != nil {
		t.Fatalf("LoadNextRangeMsgs() error = %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("len(msgs) = %d, want 3", len(msgs))
	}
}

func TestLoadNextRangeMsgsUnlimitedReadsAcrossBatches(t *testing.T) {
	store := openTestStore(t, ChannelKey{ChannelID: "c1", ChannelType: 1})
	payloads := make([]string, 0, seqReadChunkLimit+3)
	for i := 0; i < seqReadChunkLimit+3; i++ {
		payloads = append(payloads, fmt.Sprintf("m-%d", i))
	}
	mustAppendStoredMessages(t, store, payloads...)
	mustStoreCheckpoint(t, store, isr.Checkpoint{Epoch: 1, HW: uint64(len(payloads))})

	msgs, err := store.LoadNextRangeMsgs(1, 0, 0)
	if err != nil {
		t.Fatalf("LoadNextRangeMsgs() error = %v", err)
	}
	if len(msgs) != len(payloads) {
		t.Fatalf("len(msgs) = %d, want %d", len(msgs), len(payloads))
	}
	if msgs[0].MessageSeq != 1 || msgs[len(msgs)-1].MessageSeq != uint64(len(payloads)) {
		t.Fatalf("seq bounds = first:%d last:%d", msgs[0].MessageSeq, msgs[len(msgs)-1].MessageSeq)
	}
}

func TestLoadPrevRangeMsgsMatchesLegacyWindowSemantics(t *testing.T) {
	store := openTestStore(t, ChannelKey{ChannelID: "c1", ChannelType: 1})
	mustAppendStoredMessages(t, store, "one", "two", "three", "four", "five")
	mustStoreCheckpoint(t, store, isr.Checkpoint{Epoch: 1, HW: 5})

	msgs, err := store.LoadPrevRangeMsgs(5, 0, 2)
	if err != nil {
		t.Fatalf("LoadPrevRangeMsgs() error = %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("len(msgs) = %d, want 2", len(msgs))
	}
	if string(msgs[0].Payload) != "four" || string(msgs[1].Payload) != "five" {
		t.Fatalf("payloads = [%q %q], want [four five]", msgs[0].Payload, msgs[1].Payload)
	}
}

func TestLoadPrevRangeMsgsReturnsEmptyForCollapsedWindow(t *testing.T) {
	store := openTestStore(t, ChannelKey{ChannelID: "c1", ChannelType: 1})
	mustAppendStoredMessages(t, store, "one", "two", "three")
	mustStoreCheckpoint(t, store, isr.Checkpoint{Epoch: 1, HW: 3})

	msgs, err := store.LoadPrevRangeMsgs(3, 3, 10)
	if err != nil {
		t.Fatalf("LoadPrevRangeMsgs() error = %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("len(msgs) = %d, want 0", len(msgs))
	}
}

func TestLoadPrevRangeMsgsTreatsZeroLimitAsUnlimited(t *testing.T) {
	store := openTestStore(t, ChannelKey{ChannelID: "c1", ChannelType: 1})
	mustAppendStoredMessages(t, store, "one", "two", "three", "four")
	mustStoreCheckpoint(t, store, isr.Checkpoint{Epoch: 1, HW: 4})

	msgs, err := store.LoadPrevRangeMsgs(4, 0, 0)
	if err != nil {
		t.Fatalf("LoadPrevRangeMsgs() error = %v", err)
	}
	if len(msgs) != 4 {
		t.Fatalf("len(msgs) = %d, want 4", len(msgs))
	}
	if msgs[0].MessageSeq != 1 || msgs[3].MessageSeq != 4 {
		t.Fatalf("seqs = %+v", msgs)
	}
}

func TestTruncateLogToRemovesTailAndClampsCheckpoint(t *testing.T) {
	store := openTestStore(t, ChannelKey{ChannelID: "c1", ChannelType: 1})
	mustAppendStoredMessages(t, store, "one", "two", "three")
	mustStoreCheckpoint(t, store, isr.Checkpoint{Epoch: 1, HW: 3})

	if err := store.TruncateLogTo(2); err != nil {
		t.Fatalf("TruncateLogTo() error = %v", err)
	}

	got, err := store.LoadNextRangeMsgs(1, 0, 10)
	if err != nil {
		t.Fatalf("LoadNextRangeMsgs() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}

	checkpoint, err := store.loadCheckpoint()
	if err != nil {
		t.Fatalf("loadCheckpoint() error = %v", err)
	}
	if checkpoint.HW != 2 {
		t.Fatalf("HW = %d, want 2", checkpoint.HW)
	}
}

func TestTruncateLogToClearsSnapshotPayloadWhenClampingLogStartOffset(t *testing.T) {
	store := openTestStore(t, ChannelKey{ChannelID: "c1", ChannelType: 1})
	mustAppendStoredMessages(t, store, "one", "two", "three", "four", "five")
	mustStoreCheckpoint(t, store, isr.Checkpoint{Epoch: 2, LogStartOffset: 5, HW: 5})
	if err := store.storeSnapshotPayload([]byte("snapshot")); err != nil {
		t.Fatalf("storeSnapshotPayload() error = %v", err)
	}

	if err := store.TruncateLogTo(3); err != nil {
		t.Fatalf("TruncateLogTo() error = %v", err)
	}

	payload, err := store.loadSnapshotPayload()
	if err != nil {
		t.Fatalf("loadSnapshotPayload() error = %v", err)
	}
	if payload != nil {
		t.Fatalf("payload = %q, want nil", payload)
	}

	checkpoint, err := store.loadCheckpoint()
	if err != nil {
		t.Fatalf("loadCheckpoint() error = %v", err)
	}
	if checkpoint.LogStartOffset != 3 || checkpoint.HW != 3 {
		t.Fatalf("checkpoint = %+v", checkpoint)
	}
}
