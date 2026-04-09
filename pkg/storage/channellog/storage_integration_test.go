package channellog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

func TestClusterWithRealStoreAppendFetchAndSeqReads(t *testing.T) {
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

func TestClusterAppendDoesNotFailAfterAtomicCheckpointIdempotencyCommit(t *testing.T) {
	db := openTestDB(t)
	key := ChannelKey{ChannelID: "c1", ChannelType: 1}
	store := db.ForChannel(key)
	state := &atomicCheckpointOnlyStateStore{
		directPutErr: errors.New("direct put should be skipped after checkpoint commit"),
	}

	checkpoints, err := newCheckpointBridge(store.isrCheckpointStore(), store, db, key, state, channelGroupKey(key))
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

	meta := singleReplicaMeta(store.groupKey, 7, 1)
	if err := replica.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	if err := replica.BecomeLeader(meta); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	got, err := New(Config{
		Runtime: &realRuntime{
			groups: map[isr.GroupKey]GroupHandle{
				channelGroupKey(key): replica,
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

func realStoreAppendRequest(key ChannelKey, payload string) AppendRequest {
	return AppendRequest{
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

type singleStateStoreFactory struct {
	store ChannelStateStore
}

func (f *singleStateStoreFactory) ForChannel(ChannelKey) (ChannelStateStore, error) {
	return f.store, nil
}

type atomicCheckpointOnlyStateStore struct {
	idempotency    map[IdempotencyKey]IdempotencyEntry
	directPutErr   error
	directPutCalls int
	atomicCommits  int
}

func (s *atomicCheckpointOnlyStateStore) PutIdempotency(key IdempotencyKey, entry IdempotencyEntry) error {
	s.directPutCalls++
	if s.directPutErr != nil {
		return s.directPutErr
	}
	if s.idempotency == nil {
		s.idempotency = make(map[IdempotencyKey]IdempotencyEntry)
	}
	s.idempotency[key] = entry
	return nil
}

func (s *atomicCheckpointOnlyStateStore) GetIdempotency(key IdempotencyKey) (IdempotencyEntry, bool, error) {
	entry, ok := s.idempotency[key]
	return entry, ok, nil
}

func (s *atomicCheckpointOnlyStateStore) Snapshot(uint64) ([]byte, error) {
	return nil, nil
}

func (s *atomicCheckpointOnlyStateStore) Restore([]byte) error {
	return nil
}

func (s *atomicCheckpointOnlyStateStore) CommitCommittedWithCheckpoint(_ isr.Checkpoint, batch []appliedMessage) error {
	s.atomicCommits++
	if s.idempotency == nil {
		s.idempotency = make(map[IdempotencyKey]IdempotencyEntry)
	}
	for _, msg := range batch {
		s.idempotency[msg.key] = msg.entry
	}
	return nil
}
