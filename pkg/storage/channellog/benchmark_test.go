package channellog

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

const (
	benchmarkDefaultPayloadBytes     = 256
	benchmarkDefaultPreloadedMessages = 4096
	benchmarkDefaultPageLimit         = 64
	benchmarkDefaultFetchBytes        = 1 << 20
)

type benchmarkStoreFixtureConfig struct {
	preloadedMessages int
	committedHW       int
	payloadBytes      int
}

type benchmarkStoreFixture struct {
	db    *DB
	key   ChannelKey
	store *Store
}

type benchmarkRecoveredReplicaFixtureConfig struct {
	preloadedMessages int
	committedHW       int
	payloadBytes      int
}

type benchmarkRecoveredReplicaFixture struct {
	dir     string
	key     ChannelKey
	current *DB
}

type benchmarkClusterFixtureConfig struct {
	preloadedMessages int
	committedHW       int
	payloadBytes      int
}

type benchmarkClusterFixture struct {
	db      *DB
	key     ChannelKey
	store   *Store
	replica isr.Replica
	cluster *cluster
}

func newBenchmarkStoreFixture(tb testing.TB, cfg benchmarkStoreFixtureConfig) *benchmarkStoreFixture {
	tb.Helper()

	db := openTestDB(tb)
	key := ChannelKey{ChannelID: "bench-c1", ChannelType: 1}
	store := db.ForChannel(key)

	preloadBenchmarkStore(tb, store, cfg.preloadedMessages, cfg.committedHW, cfg.payloadBytes)
	return &benchmarkStoreFixture{
		db:    db,
		key:   key,
		store: store,
	}
}

func newBenchmarkRecoveredReplicaFixture(tb testing.TB, cfg benchmarkRecoveredReplicaFixtureConfig) *benchmarkRecoveredReplicaFixture {
	tb.Helper()

	dir := tb.TempDir()
	db, err := Open(dir)
	if err != nil {
		tb.Fatalf("Open() error = %v", err)
	}
	store := db.ForChannel(ChannelKey{ChannelID: "bench-c1", ChannelType: 1})
	preloadBenchmarkStore(tb, store, cfg.preloadedMessages, cfg.committedHW, cfg.payloadBytes)
	if err := db.Close(); err != nil {
		tb.Fatalf("Close() error = %v", err)
	}

	return &benchmarkRecoveredReplicaFixture{
		dir: dir,
		key: store.key,
	}
}

func (f *benchmarkRecoveredReplicaFixture) OpenReplica(tb testing.TB) isr.Replica {
	tb.Helper()

	if f.current != nil {
		tb.Fatalf("OpenReplica() called with an already-open DB")
	}
	db, err := Open(f.dir)
	if err != nil {
		tb.Fatalf("Open() error = %v", err)
	}
	f.current = db
	return newStoreReplica(tb, db.ForChannel(f.key), 1)
}

func (f *benchmarkRecoveredReplicaFixture) CloseReplica(tb testing.TB) {
	tb.Helper()
	if f.current == nil {
		return
	}
	if err := f.current.Close(); err != nil {
		tb.Fatalf("Close() error = %v", err)
	}
	f.current = nil
}

func (f *benchmarkRecoveredReplicaFixture) Close() {
	if f.current == nil {
		return
	}
	_ = f.current.Close()
	f.current = nil
}

func newBenchmarkClusterFixture(tb testing.TB, cfg benchmarkClusterFixtureConfig) *benchmarkClusterFixture {
	tb.Helper()

	db := openTestDB(tb)
	key := ChannelKey{ChannelID: "bench-c1", ChannelType: 1}
	store := db.ForChannel(key)
	preloadBenchmarkStore(tb, store, cfg.preloadedMessages, cfg.committedHW, cfg.payloadBytes)

	replica := newStoreReplica(tb, store, 1)
	meta := singleReplicaMeta(store.groupKey, 1, 1)
	if err := replica.ApplyMeta(meta); err != nil {
		tb.Fatalf("ApplyMeta() error = %v", err)
	}
	if err := replica.BecomeLeader(meta); err != nil {
		tb.Fatalf("BecomeLeader() error = %v", err)
	}

	cluster := newRealStoreCluster(tb, db, key, replica, 1, 1)
	return &benchmarkClusterFixture{
		db:      db,
		key:     key,
		store:   store,
		replica: replica,
		cluster: cluster,
	}
}

func preloadBenchmarkStore(tb testing.TB, store *Store, preloadedMessages, committedHW, payloadBytes int) {
	tb.Helper()

	if preloadedMessages <= 0 {
		return
	}
	encoded := make([][]byte, 0, preloadedMessages)
	payload := benchmarkPayload(payloadBytes)
	for i := 0; i < preloadedMessages; i++ {
		encoded = append(encoded, mustBenchmarkStoredPayload(tb, uint64(i+1), payload))
	}
	if _, err := store.appendPayloads(encoded); err != nil {
		tb.Fatalf("appendPayloads() error = %v", err)
	}

	hw := committedHW
	if hw <= 0 {
		hw = preloadedMessages
	}
	if err := store.storeCheckpoint(isr.Checkpoint{
		Epoch:          1,
		LogStartOffset: 0,
		HW:             uint64(hw),
	}); err != nil {
		tb.Fatalf("storeCheckpoint() error = %v", err)
	}
}

func benchmarkPayload(size int) []byte {
	if size <= 0 {
		size = benchmarkDefaultPayloadBytes
	}
	return bytes.Repeat([]byte("p"), size)
}

func mustBenchmarkStoredPayload(tb testing.TB, messageID uint64, payload []byte) []byte {
	tb.Helper()

	encoded, err := encodeStoredMessage(storedMessage{
		MessageID:   messageID,
		SenderUID:   "bench-u1",
		ClientMsgNo: fmt.Sprintf("bench-m%d", messageID),
		PayloadHash: hashPayload(payload),
		Payload:     append([]byte(nil), payload...),
	})
	if err != nil {
		tb.Fatalf("encodeStoredMessage() error = %v", err)
	}
	return encoded
}

func benchmarkReadOffset(iteration, total, pageLimit int) uint64 {
	if total <= pageLimit {
		return 0
	}
	return uint64(iteration % (total - pageLimit + 1))
}

func benchmarkReadSeq(iteration, total, pageLimit int) uint64 {
	return benchmarkReadOffset(iteration, total, pageLimit) + 1
}

func benchmarkTailSeq(iteration, total, pageLimit int) uint64 {
	if total <= pageLimit {
		return uint64(total)
	}
	return benchmarkReadOffset(iteration, total, pageLimit) + uint64(pageLimit)
}

func benchmarkSendRequest(key ChannelKey, payload []byte) SendRequest {
	return SendRequest{
		ChannelID:   key.ChannelID,
		ChannelType: key.ChannelType,
		Message: Message{
			ChannelID:   key.ChannelID,
			ChannelType: key.ChannelType,
			FromUID:     "bench-u1",
			Payload:     payload,
		},
	}
}
