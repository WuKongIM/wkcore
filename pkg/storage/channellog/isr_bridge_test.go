package channellog

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestStoreBridgesReplicaAppendAndRecoverCommittedLog(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
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

	result, err := replica.Append(context.Background(), []isr.Record{
		mustStoredRecord(t, 1, "one"),
		mustStoredRecord(t, 2, "two"),
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if result.BaseOffset != 0 || result.NextCommitHW != 2 {
		t.Fatalf("commit result = %+v", result)
	}

	msgs, err := store.LoadNextRangeMsgs(1, 0, 10)
	if err != nil {
		t.Fatalf("LoadNextRangeMsgs() error = %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("len(msgs) = %d, want 2", len(msgs))
	}
	if string(msgs[0].Payload) != "one" || string(msgs[1].Payload) != "two" {
		t.Fatalf("payloads = [%q %q]", msgs[0].Payload, msgs[1].Payload)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(reopen) error = %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("Close(reopen) error = %v", err)
		}
	}()

	reloadedStore := reopened.ForChannel(key)
	reloadedReplica := newStoreReplica(t, reloadedStore, 1)
	status := reloadedReplica.Status()
	if status.HW != 2 || status.LEO != 2 {
		t.Fatalf("status = %+v", status)
	}

	msg, err := reloadedStore.LoadMsg(2)
	if err != nil {
		t.Fatalf("LoadMsg(2) error = %v", err)
	}
	if string(msg.Payload) != "two" {
		t.Fatalf("Payload = %q, want %q", msg.Payload, "two")
	}
}

func TestStoreBridgesReplicaApplyFetchAndRecoverFollowerState(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	key := ChannelKey{ChannelID: "c1", ChannelType: 1}
	store := db.ForChannel(key)

	replica := newChannelLogReplica(t, store, 2)
	meta := followerMeta(store.groupKey, 7, 1, 2)
	if err := replica.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	if err := replica.BecomeFollower(meta); err != nil {
		t.Fatalf("BecomeFollower() error = %v", err)
	}

	if err := replica.ApplyFetch(context.Background(), isr.ApplyFetchRequest{
		GroupKey: meta.GroupKey,
		Epoch:    meta.Epoch,
		Leader:   meta.Leader,
		Records: []isr.Record{
			mustStoredRecord(t, 1, "one"),
			mustStoredRecord(t, 2, "two"),
		},
		LeaderHW: 10,
	}); err != nil {
		t.Fatalf("ApplyFetch() error = %v", err)
	}

	checkpoint, err := store.loadCheckpoint()
	if err != nil {
		t.Fatalf("loadCheckpoint() error = %v", err)
	}
	if checkpoint.HW != 2 {
		t.Fatalf("checkpoint = %+v", checkpoint)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(reopen) error = %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("Close(reopen) error = %v", err)
		}
	}()

	reloadedStore := reopened.ForChannel(key)
	reloadedReplica := newStoreReplica(t, reloadedStore, 2)
	status := reloadedReplica.Status()
	if status.HW != 2 || status.LEO != 2 {
		t.Fatalf("status = %+v", status)
	}

	msgs, err := reloadedStore.LoadNextRangeMsgs(1, 0, 10)
	if err != nil {
		t.Fatalf("LoadNextRangeMsgs() error = %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("len(msgs) = %d, want 2", len(msgs))
	}
}

func TestStoreReplicaApplyFetchUsesSingleDurableCommit(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	key := ChannelKey{ChannelID: "c1", ChannelType: 1}
	store := db.ForChannel(key)
	replica := newChannelLogReplica(t, store, 2)
	meta := followerMeta(store.groupKey, 7, 1, 2)
	if err := replica.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	if err := replica.BecomeFollower(meta); err != nil {
		t.Fatalf("BecomeFollower() error = %v", err)
	}

	before := store.durableCommitCount.Load()
	if err := replica.ApplyFetch(context.Background(), isr.ApplyFetchRequest{
		GroupKey: meta.GroupKey,
		Epoch:    meta.Epoch,
		Leader:   meta.Leader,
		Records: []isr.Record{
			mustStoredRecord(t, 1, "one"),
			mustStoredRecord(t, 2, "two"),
		},
		LeaderHW: 2,
	}); err != nil {
		t.Fatalf("ApplyFetch() error = %v", err)
	}
	after := store.durableCommitCount.Load()
	if got := after - before; got != 1 {
		t.Fatalf("durable commit delta = %d, want 1", got)
	}
}

func TestChannelLogReplicaAppendUsesSingleDurableSync(t *testing.T) {
	fs := newCountingFS(vfs.NewMem())
	pdb, err := pebble.Open("test", &pebble.Options{FS: fs})
	if err != nil {
		t.Fatalf("pebble.Open() error = %v", err)
	}
	db := &DB{
		db:     pdb,
		stores: make(map[ChannelKey]*Store),
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	key := ChannelKey{ChannelID: "c1", ChannelType: 1}
	store := db.ForChannel(key)
	replica := newChannelLogReplica(t, store, 1)
	meta := singleReplicaMeta(store.groupKey, 7, 1)
	if err := replica.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	if err := replica.BecomeLeader(meta); err != nil {
		t.Fatalf("BecomeLeader() error = %v", err)
	}

	before := fs.syncCount.Load()
	if _, err := replica.Append(context.Background(), []isr.Record{
		mustStoredRecord(t, 1, "one"),
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	after := fs.syncCount.Load()
	if got := after - before; got != 1 {
		t.Fatalf("sync count delta = %d, want 1", got)
	}
}

func TestStoreBridgesReplicaInstallSnapshotPersistsPayloadAndOffsets(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
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
	if _, err := replica.Append(context.Background(), []isr.Record{
		mustStoredRecord(t, 1, "one"),
		mustStoredRecord(t, 2, "two"),
		mustStoredRecord(t, 3, "three"),
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	snap := isr.Snapshot{
		GroupKey:  meta.GroupKey,
		Epoch:     9,
		EndOffset: 3,
		Payload:   []byte("snapshot"),
	}
	if err := replica.InstallSnapshot(context.Background(), snap); err != nil {
		t.Fatalf("InstallSnapshot() error = %v", err)
	}

	payload, err := store.loadSnapshotPayload()
	if err != nil {
		t.Fatalf("loadSnapshotPayload() error = %v", err)
	}
	if !bytes.Equal(payload, snap.Payload) {
		t.Fatalf("snapshot payload = %q, want %q", payload, snap.Payload)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(reopen) error = %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("Close(reopen) error = %v", err)
		}
	}()

	reloadedStore := reopened.ForChannel(key)
	reloadedReplica := newStoreReplica(t, reloadedStore, 1)
	status := reloadedReplica.Status()
	if status.LogStartOffset != 3 || status.HW != 3 || status.LEO != 3 {
		t.Fatalf("status = %+v", status)
	}
}

func TestStoreRecoveryTrimsEpochHistoryAlongsideDirtyTail(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	key := ChannelKey{ChannelID: "c1", ChannelType: 1}
	store := db.ForChannel(key)

	if _, err := store.appendPayloads([][]byte{
		[]byte("one"),
		[]byte("two"),
		[]byte("three"),
		[]byte("four"),
		[]byte("five"),
		[]byte("six"),
	}); err != nil {
		t.Fatalf("appendPayloads() error = %v", err)
	}
	if err := store.storeCheckpoint(isr.Checkpoint{
		Epoch:          7,
		LogStartOffset: 0,
		HW:             4,
	}); err != nil {
		t.Fatalf("storeCheckpoint() error = %v", err)
	}
	if err := store.appendEpochPoint(isr.EpochPoint{Epoch: 7, StartOffset: 0}); err != nil {
		t.Fatalf("appendEpochPoint(7@0) error = %v", err)
	}
	if err := store.appendEpochPoint(isr.EpochPoint{Epoch: 8, StartOffset: 5}); err != nil {
		t.Fatalf("appendEpochPoint(8@5) error = %v", err)
	}

	replica := newStoreReplica(t, store, 1)
	status := replica.Status()
	if status.HW != 4 || status.LEO != 4 {
		t.Fatalf("status = %+v", status)
	}

	points, err := store.loadEpochHistory()
	if err != nil {
		t.Fatalf("loadEpochHistory() error = %v", err)
	}
	if want := []isr.EpochPoint{{Epoch: 7, StartOffset: 0}}; !reflect.DeepEqual(points, want) {
		t.Fatalf("epoch history = %+v, want %+v", points, want)
	}
}

func TestISRLogStoreBridgeLEOReturnsLastKnownOffsetAfterClose(t *testing.T) {
	db := openTestDB(t)
	key := ChannelKey{ChannelID: "c1", ChannelType: 1}
	store := db.ForChannel(key)
	if _, err := store.appendPayloads([][]byte{[]byte("one"), []byte("two")}); err != nil {
		t.Fatalf("appendPayloads() error = %v", err)
	}

	logStore := store.isrLogStore()
	if got := logStore.LEO(); got != 2 {
		t.Fatalf("LEO() = %d, want 2", got)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := logStore.LEO(); got != 2 {
		t.Fatalf("LEO() after Close = %d, want 2", got)
	}
}

func newStoreReplica(t testing.TB, store *Store, localNode isr.NodeID) isr.Replica {
	t.Helper()

	replica, err := isr.NewReplica(isr.ReplicaConfig{
		LocalNode:         localNode,
		LogStore:          store.isrLogStore(),
		CheckpointStore:   store.isrCheckpointStore(),
		EpochHistoryStore: store.isrEpochHistoryStore(),
		SnapshotApplier:   store.isrSnapshotApplier(),
		Now:               func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
	})
	if err != nil {
		t.Fatalf("NewReplica() error = %v", err)
	}
	return replica
}

func newChannelLogReplica(t testing.TB, store *Store, localNode isr.NodeID) isr.Replica {
	t.Helper()

	replica, err := NewReplica(store, localNode, func() time.Time {
		return time.Unix(1_700_000_000, 0).UTC()
	})
	if err != nil {
		t.Fatalf("NewReplica() error = %v", err)
	}
	return replica
}

func mustStoredRecord(t testing.TB, messageID uint64, payload string) isr.Record {
	t.Helper()

	encoded, err := encodeStoredMessage(storedMessage{
		MessageID:   messageID,
		FromUID:     "u1",
		ClientMsgNo: payload,
		PayloadHash: hashPayload([]byte(payload)),
		Payload:     []byte(payload),
	})
	if err != nil {
		t.Fatalf("encodeStoredMessage() error = %v", err)
	}
	return isr.Record{
		Payload:   encoded,
		SizeBytes: len(encoded),
	}
}

func singleReplicaMeta(groupKey isr.GroupKey, epoch uint64, leader isr.NodeID) isr.GroupMeta {
	return isr.GroupMeta{
		GroupKey:   groupKey,
		Epoch:      epoch,
		Leader:     leader,
		Replicas:   []isr.NodeID{leader},
		ISR:        []isr.NodeID{leader},
		MinISR:     1,
		LeaseUntil: time.Unix(1_700_000_300, 0).UTC(),
	}
}

func followerMeta(groupKey isr.GroupKey, epoch uint64, leader, localNode isr.NodeID) isr.GroupMeta {
	return isr.GroupMeta{
		GroupKey:   groupKey,
		Epoch:      epoch,
		Leader:     leader,
		Replicas:   []isr.NodeID{leader, localNode},
		ISR:        []isr.NodeID{leader, localNode},
		MinISR:     1,
		LeaseUntil: time.Unix(1_700_000_300, 0).UTC(),
	}
}
