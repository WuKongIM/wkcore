package channellog

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

func TestClusterWithRealStoreSendFetchAndSeqReads(t *testing.T) {
	db := openTestDB(t)
	key := ChannelKey{ChannelID: "c1", ChannelType: 1}
	store := db.ForChannel(key)

	replica := newStoreReplica(t, store, 1)
	meta := singleReplicaMeta(store.groupKey, 7, 1)
	if err := replica.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	if err := replica.BecomeLeader(meta); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	cluster := newRealStoreCluster(t, db, key, replica, 3, 7)
	first, err := cluster.Send(context.Background(), realStoreSendRequest(key, "one"))
	if err != nil {
		t.Fatalf("first Send() error = %v", err)
	}
	second, err := cluster.Send(context.Background(), realStoreSendRequest(key, "two"))
	if err != nil {
		t.Fatalf("second Send() error = %v", err)
	}
	duplicate, err := cluster.Send(context.Background(), realStoreSendRequest(key, "one"))
	if err != nil {
		t.Fatalf("duplicate Send() error = %v", err)
	}
	if first.MessageSeq != 1 || second.MessageSeq != 2 {
		t.Fatalf("send results = first:%+v second:%+v", first, second)
	}
	if duplicate.MessageID != first.MessageID || duplicate.MessageSeq != first.MessageSeq {
		t.Fatalf("duplicate send result = %+v, want %+v", duplicate, first)
	}
	if duplicate.Message.MessageID != first.Message.MessageID || duplicate.Message.MessageSeq != first.Message.MessageSeq {
		t.Fatalf("duplicate committed message = %+v, want %+v", duplicate.Message, first.Message)
	}

	result, err := cluster.Fetch(context.Background(), FetchRequest{
		Key:      key,
		FromSeq:  1,
		Limit:    10,
		MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(result.Messages) != 2 {
		t.Fatalf("len(Messages) = %d, want 2", len(result.Messages))
	}
	if result.Messages[0].MessageSeq != 1 || result.Messages[1].MessageSeq != 2 {
		t.Fatalf("message seqs = %+v", result.Messages)
	}
	if result.NextSeq != 3 || result.CommittedSeq != 2 {
		t.Fatalf("fetch result = %+v", result)
	}

	msgs, err := store.LoadNextRangeMsgs(1, 0, 10)
	if err != nil {
		t.Fatalf("LoadNextRangeMsgs() error = %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("len(msgs) = %d, want 2", len(msgs))
	}
	if msgs[0].MessageSeq != 1 || msgs[1].MessageSeq != 2 {
		t.Fatalf("store seqs = %+v", msgs)
	}

	stateStore, err := db.StateStoreFactory().ForChannel(key)
	if err != nil {
		t.Fatalf("StateStoreFactory().ForChannel() error = %v", err)
	}
	entry, ok, err := stateStore.GetIdempotency(IdempotencyKey{
		ChannelID:   key.ChannelID,
		ChannelType: key.ChannelType,
		SenderUID:   "u1",
		ClientMsgNo: "msg-one",
	})
	if err != nil {
		t.Fatalf("GetIdempotency() error = %v", err)
	}
	if !ok {
		t.Fatal("expected persisted idempotency entry")
	}
	if entry.MessageSeq != 1 || entry.Offset != 0 {
		t.Fatalf("idempotency entry = %+v", entry)
	}
}

func TestClusterWithRealStoreFetchHidesDirtyTailAfterReplicaRecovery(t *testing.T) {
	db := openTestDB(t)
	key := ChannelKey{ChannelID: "c1", ChannelType: 1}
	store := db.ForChannel(key)

	logStore := store.isrLogStore()
	if _, err := logStore.Append([]isr.Record{
		mustStoredRecord(t, 1, "one"),
		mustStoredRecord(t, 2, "two"),
		mustStoredRecord(t, 3, "three"),
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := logStore.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if err := store.isrCheckpointStore().Store(isr.Checkpoint{
		Epoch:          7,
		LogStartOffset: 0,
		HW:             2,
	}); err != nil {
		t.Fatalf("Store(checkpoint) error = %v", err)
	}

	replica := newStoreReplica(t, store, 1)
	meta := singleReplicaMeta(store.groupKey, 7, 1)
	if err := replica.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	if err := replica.BecomeLeader(meta); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	cluster := newRealStoreCluster(t, db, key, replica, 3, 7)
	result, err := cluster.Fetch(context.Background(), FetchRequest{
		Key:      key,
		FromSeq:  1,
		Limit:    10,
		MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(result.Messages) != 2 {
		t.Fatalf("len(Messages) = %d, want 2", len(result.Messages))
	}
	if result.NextSeq != 3 || result.CommittedSeq != 2 {
		t.Fatalf("fetch result = %+v", result)
	}
	if status := replica.Status(); status.LEO != 2 || status.HW != 2 {
		t.Fatalf("status = %+v", status)
	}

	_, err = store.LoadMsg(3)
	if !errors.Is(err, ErrMessageNotFound) {
		t.Fatalf("expected ErrMessageNotFound, got %v", err)
	}
}

type realRuntime struct {
	groups map[isr.GroupKey]GroupHandle
}

func (r *realRuntime) Group(groupKey isr.GroupKey) (GroupHandle, bool) {
	group, ok := r.groups[groupKey]
	return group, ok
}

func newRealStoreCluster(t testing.TB, db *DB, key ChannelKey, replica isr.Replica, channelEpoch, leaderEpoch uint64) *cluster {
	t.Helper()

	got, err := New(Config{
		Runtime: &realRuntime{
			groups: map[isr.GroupKey]GroupHandle{
				channelGroupKey(key): replica,
			},
		},
		Log:        db,
		States:     db.StateStoreFactory(),
		MessageIDs: &fakeMessageIDGenerator{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	c := got.(*cluster)
	if err := c.ApplyMeta(realStoreChannelMeta(key, channelEpoch, leaderEpoch)); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	return c
}

func realStoreSendRequest(key ChannelKey, payload string) SendRequest {
	return SendRequest{
		ChannelID:   key.ChannelID,
		ChannelType: key.ChannelType,
		Message: Message{
			ChannelID:   key.ChannelID,
			ChannelType: key.ChannelType,
			FromUID:     "u1",
			ClientMsgNo: "msg-" + payload,
			Payload:     []byte(payload),
		},
	}
}

func realStoreChannelMeta(key ChannelKey, channelEpoch, leaderEpoch uint64) ChannelMeta {
	return ChannelMeta{
		ChannelID:    key.ChannelID,
		ChannelType:  key.ChannelType,
		ChannelEpoch: channelEpoch,
		LeaderEpoch:  leaderEpoch,
		Replicas:     []NodeID{1},
		ISR:          []NodeID{1},
		Leader:       1,
		MinISR:       1,
		Status:       ChannelStatusActive,
		Features: ChannelFeatures{
			MessageSeqFormat: MessageSeqFormatLegacyU32,
		},
	}
}
