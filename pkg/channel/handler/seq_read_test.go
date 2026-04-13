package handler

import (
	"errors"
	"testing"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
)

func TestLoadMsgReturnsCommittedMessageOnly(t *testing.T) {
	id := core.ChannelID{ID: "c1", Type: 1}
	engine := openTestEngine(t)
	st := engine.ForChannel(KeyFromChannelID(id), id)
	mustAppendEncodedMessages(t, st,
		core.Message{MessageID: 11, ChannelID: id.ID, ChannelType: id.Type, FromUID: "u1", ClientMsgNo: "m1", Payload: []byte("one")},
		core.Message{MessageID: 12, ChannelID: id.ID, ChannelType: id.Type, FromUID: "u1", ClientMsgNo: "m2", Payload: []byte("two")},
		core.Message{MessageID: 13, ChannelID: id.ID, ChannelType: id.Type, FromUID: "u1", ClientMsgNo: "m3", Payload: []byte("three")},
	)
	if err := st.StoreCheckpoint(core.Checkpoint{Epoch: 1, HW: 2}); err != nil {
		t.Fatalf("StoreCheckpoint() error = %v", err)
	}

	msg, err := LoadMsg(st, 2)
	if err != nil {
		t.Fatalf("LoadMsg(2) error = %v", err)
	}
	if string(msg.Payload) != "two" {
		t.Fatalf("Payload = %q, want %q", msg.Payload, "two")
	}
	if msg.MessageSeq != 2 {
		t.Fatalf("MessageSeq = %d, want 2", msg.MessageSeq)
	}

	_, err = LoadMsg(st, 3)
	if !errors.Is(err, core.ErrMessageNotFound) {
		t.Fatalf("expected ErrMessageNotFound, got %v", err)
	}
}

func TestLoadNextRangeMsgsCapsByCommittedHW(t *testing.T) {
	id := core.ChannelID{ID: "c1", Type: 1}
	engine := openTestEngine(t)
	st := engine.ForChannel(KeyFromChannelID(id), id)
	mustAppendEncodedMessages(t, st,
		core.Message{MessageID: 11, ChannelID: id.ID, ChannelType: id.Type, Payload: []byte("one")},
		core.Message{MessageID: 12, ChannelID: id.ID, ChannelType: id.Type, Payload: []byte("two")},
		core.Message{MessageID: 13, ChannelID: id.ID, ChannelType: id.Type, Payload: []byte("three")},
	)
	if err := st.StoreCheckpoint(core.Checkpoint{Epoch: 1, HW: 2}); err != nil {
		t.Fatalf("StoreCheckpoint() error = %v", err)
	}

	msgs, err := LoadNextRangeMsgs(st, 1, 0, 10)
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
