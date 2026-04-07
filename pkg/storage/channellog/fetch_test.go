package channellog

import (
	"context"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

func TestFetchFromSeqOneReturnsCommittedMessagesOnly(t *testing.T) {
	env := newFetchEnv(t)

	result, err := env.cluster.Fetch(context.Background(), FetchRequest{
		Key:      env.key,
		FromSeq:  1,
		Limit:    10,
		MaxBytes: 1024,
	})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(result.Messages) != 2 {
		t.Fatalf("len(Messages) = %d, want 2", len(result.Messages))
	}
	if result.NextSeq != 3 {
		t.Fatalf("NextSeq = %d, want 3", result.NextSeq)
	}
	if result.CommittedSeq != 2 {
		t.Fatalf("CommittedSeq = %d, want 2", result.CommittedSeq)
	}
}

func TestFetchRejectsInvalidBudget(t *testing.T) {
	env := newFetchEnv(t)

	_, err := env.cluster.Fetch(context.Background(), FetchRequest{
		Key: env.key, FromSeq: 1, Limit: 1, MaxBytes: 0,
	})
	if err != ErrInvalidFetchBudget {
		t.Fatalf("expected ErrInvalidFetchBudget, got %v", err)
	}
}

func TestFetchReturnsDurableMessageFields(t *testing.T) {
	env := newFetchEnv(t)

	result, err := env.cluster.Fetch(context.Background(), FetchRequest{
		Key:      env.key,
		FromSeq:  1,
		Limit:    1,
		MaxBytes: 1024,
	})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(result.Messages) != 1 {
		t.Fatalf("len(Messages) = %d, want 1", len(result.Messages))
	}

	msg := result.Messages[0]
	if msg.FromUID != "u1" {
		t.Fatalf("FromUID = %q, want %q", msg.FromUID, "u1")
	}
	if msg.MsgKey != "k1" {
		t.Fatalf("MsgKey = %q, want %q", msg.MsgKey, "k1")
	}
}

type fetchEnv struct {
	cluster *cluster
	key     ChannelKey
}

func newFetchEnv(t *testing.T) *fetchEnv {
	t.Helper()

	log := &fakeMessageLog{
		records: []LogRecord{
			{
				Offset: 0,
				Payload: mustEncodeDurableMessage(t, Message{
					MessageID:   11,
					Framer:      wkframe.Framer{RedDot: true},
					Setting:     1,
					MsgKey:      "k1",
					ClientSeq:   101,
					ClientMsgNo: "m1",
					StreamNo:    "stream-1",
					StreamID:    1001,
					StreamFlag:  wkframe.StreamFlagEnd,
					Timestamp:   111,
					ChannelID:   "c1",
					ChannelType: 1,
					Topic:       "topic-1",
					FromUID:     "u1",
					Payload:     []byte("one"),
				}),
			},
			{
				Offset: 1,
				Payload: mustEncodeDurableMessage(t, Message{
					MessageID:   12,
					MsgKey:      "k2",
					ClientSeq:   102,
					ClientMsgNo: "m2",
					ChannelID:   "c1",
					ChannelType: 1,
					Topic:       "topic-2",
					FromUID:     "u1",
					Payload:     []byte("two"),
				}),
			},
			{
				Offset: 2,
				Payload: mustEncodeDurableMessage(t, Message{
					MessageID:   13,
					MsgKey:      "k3",
					ClientSeq:   103,
					ClientMsgNo: "m3",
					ChannelID:   "c1",
					ChannelType: 1,
					Topic:       "topic-3",
					FromUID:     "u1",
					Payload:     []byte("three"),
				}),
			},
		},
	}
	group := &fakeGroupHandle{
		state: isr.ReplicaState{
			GroupKey:       channelGroupKey(ChannelKey{ChannelID: "c1", ChannelType: 1}),
			Role:           isr.RoleLeader,
			Epoch:          9,
			Leader:         1,
			LogStartOffset: 0,
			HW:             2,
		},
	}
	got, err := New(Config{
		Runtime: &fakeRuntime{
			groups: map[isr.GroupKey]*fakeGroupHandle{
				channelGroupKey(ChannelKey{ChannelID: "c1", ChannelType: 1}): group,
			},
		},
		Log:        log,
		States:     &fakeStateStoreFactory{},
		MessageIDs: &fakeMessageIDGenerator{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	c := got.(*cluster)
	meta := testMeta("c1", 1, 3, 9)
	if err := c.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	return &fetchEnv{
		cluster: c,
		key: ChannelKey{
			ChannelID:   "c1",
			ChannelType: 1,
		},
	}
}
