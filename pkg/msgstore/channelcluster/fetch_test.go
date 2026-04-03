package channelcluster

import (
	"context"
	"testing"

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
			{
				Offset: 2,
				Payload: mustEncodeStoredMessage(t, storedMessage{
					MessageID:   13,
					SenderUID:   "u1",
					ClientMsgNo: "m3",
					PayloadHash: hashPayload([]byte("three")),
					Payload:     []byte("three"),
				}),
			},
		},
	}
	group := &fakeGroupHandle{
		state: isr.ReplicaState{
			GroupID:        7,
			Role:           isr.RoleLeader,
			Epoch:          9,
			Leader:         1,
			LogStartOffset: 0,
			HW:             2,
		},
	}
	got, err := New(Config{
		Runtime: &fakeRuntime{
			groups: map[uint64]*fakeGroupHandle{7: group},
		},
		Log:        log,
		States:     &fakeStateStoreFactory{},
		MessageIDs: &fakeMessageIDGenerator{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	c := got.(*cluster)
	meta := testMeta("c1", 1, 7, 3, 9)
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
