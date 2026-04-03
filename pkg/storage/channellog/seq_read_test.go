package channellog

import (
	"errors"
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
