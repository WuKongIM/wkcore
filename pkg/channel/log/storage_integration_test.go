//go:build integration
// +build integration

package log

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
)

func TestClusterWithRealStoreAppendFetchAndSeqReads(t *testing.T) {
	db := openTestDB(t)
	key := ChannelKey{ChannelID: "c1", ChannelType: 1}
	store := db.ForChannel(key)

	replica := newChannelLogReplica(t, store, 1)
	meta := singleReplicaMeta(store.channelKey, 7, 1)
	if err := replica.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	if err := replica.BecomeLeader(meta); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	cluster := newRealStoreCluster(t, db, key, replica, 3, 7)
	first, err := cluster.Append(context.Background(), realStoreAppendRequest(key, "one"))
	if err != nil {
		t.Fatalf("first Append() error = %v", err)
	}
	second, err := cluster.Append(context.Background(), realStoreAppendRequest(key, "two"))
	if err != nil {
		t.Fatalf("second Append() error = %v", err)
	}
	duplicate, err := cluster.Append(context.Background(), realStoreAppendRequest(key, "one"))
	if err != nil {
		t.Fatalf("duplicate Append() error = %v", err)
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
		FromUID:     "u1",
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
	meta := singleReplicaMeta(store.channelKey, 7, 1)
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

func TestCheckpointBridgeUsesCoordinatorForCommittedState(t *testing.T) {
	db, fs := openBlockingSyncTestDB(t)
	key := ChannelKey{ChannelID: "c1", ChannelType: 1}
	store := db.ForChannel(key)
	db.commitCoordinator().flushWindow = 0

	payload, err := encodeMessage(Message{
		MessageID:   11,
		ChannelID:   key.ChannelID,
		ChannelType: key.ChannelType,
		FromUID:     "u1",
		ClientMsgNo: "msg-one",
		Payload:     []byte("one"),
	})
	if err != nil {
		t.Fatalf("encodeMessage() error = %v", err)
	}
	if _, err := store.appendPayloadsNoSync([][]byte{payload}); err != nil {
		t.Fatalf("appendPayloadsNoSync() error = %v", err)
	}

	state, err := db.StateStoreFactory().ForChannel(key)
	if err != nil {
		t.Fatalf("StateStoreFactory().ForChannel() error = %v", err)
	}
	checkpoints, err := newCheckpointBridge(store.isrCheckpointStore(), store, db, key, state, store.channelKey)
	if err != nil {
		t.Fatalf("newCheckpointBridge() error = %v", err)
	}

	done := make(chan error, 1)
	fs.enableNextSyncBlock()
	go func() {
		done <- checkpoints.Store(isr.Checkpoint{Epoch: 3, HW: 1})
	}()

	select {
	case <-fs.syncStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for checkpoint bridge commit to reach sync")
	}

	if checkpoints.prevHW != 0 {
		close(fs.syncContinue)
		<-done
		t.Fatalf("checkpoint bridge prevHW before sync = %d, want 0", checkpoints.prevHW)
	}

	close(fs.syncContinue)
	if err := <-done; err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	entry, ok, err := state.GetIdempotency(IdempotencyKey{
		ChannelID:   key.ChannelID,
		ChannelType: key.ChannelType,
		FromUID:     "u1",
		ClientMsgNo: "msg-one",
	})
	if err != nil {
		t.Fatalf("GetIdempotency() after sync error = %v", err)
	}
	if !ok {
		t.Fatal("expected idempotency entry after sync")
	}
	if entry.MessageSeq != 1 || entry.Offset != 0 {
		t.Fatalf("idempotency entry after sync = %+v", entry)
	}
	if checkpoints.prevHW != 1 {
		t.Fatalf("checkpoint bridge prevHW after sync = %d, want 1", checkpoints.prevHW)
	}
	checkpoint, err := store.loadCheckpoint()
	if err != nil {
		t.Fatalf("loadCheckpoint() after sync error = %v", err)
	}
	if checkpoint.HW != 1 {
		t.Fatalf("checkpoint after sync = %+v, want HW=1", checkpoint)
	}
}

func TestClusterAppendDoesNotFailAfterAtomicCheckpointIdempotencyCommit(t *testing.T) {
	db := openTestDB(t)
	key := ChannelKey{ChannelID: "c1", ChannelType: 1}
	store := db.ForChannel(key)
	state := &atomicCheckpointOnlyStateStore{
		directPutErr: errors.New("direct put should be skipped after checkpoint commit"),
	}

	checkpoints, err := newCheckpointBridge(store.isrCheckpointStore(), store, db, key, state, isrChannelKeyForChannel(key))
	if err != nil {
		t.Fatalf("newCheckpointBridge() error = %v", err)
	}
	replica, err := isr.NewReplica(isr.ReplicaConfig{
		LocalNode:         1,
		LogStore:          store.isrLogStore(),
		CheckpointStore:   checkpoints,
		EpochHistoryStore: store.isrEpochHistoryStore(),
		SnapshotApplier:   newSnapshotBridge(store.isrSnapshotApplier(), state),
		Now:               func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
	})
	if err != nil {
		t.Fatalf("NewReplica() error = %v", err)
	}

	meta := singleReplicaMeta(store.channelKey, 7, 1)
	if err := replica.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	if err := replica.BecomeLeader(meta); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	got, err := New(Config{
		Runtime: &realRuntime{
			groups: map[isr.ChannelKey]ChannelHandle{
				isrChannelKeyForChannel(key): replica,
			},
		},
		Log:        db,
		States:     &singleStateStoreFactory{store: state},
		MessageIDs: &fakeMessageIDGenerator{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	cluster := got.(*cluster)
	if err := cluster.ApplyMeta(realStoreChannelMeta(key, 3, 7)); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}

	result, err := cluster.Append(context.Background(), realStoreAppendRequest(key, "one"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if result.MessageSeq != 1 {
		t.Fatalf("MessageSeq = %d, want 1", result.MessageSeq)
	}
	if state.atomicCommits != 1 {
		t.Fatalf("atomicCommits = %d, want 1", state.atomicCommits)
	}
	if state.directPutCalls != 0 {
		t.Fatalf("directPutCalls = %d, want 0", state.directPutCalls)
	}

	entry, ok, err := state.GetIdempotency(IdempotencyKey{
		ChannelID:   key.ChannelID,
		ChannelType: key.ChannelType,
		FromUID:     "u1",
		ClientMsgNo: "msg-one",
	})
	if err != nil {
		t.Fatalf("GetIdempotency() error = %v", err)
	}
	if !ok {
		t.Fatal("expected idempotency entry")
	}
	if entry.MessageSeq != 1 || entry.Offset != 0 {
		t.Fatalf("idempotency entry = %+v", entry)
	}
}
