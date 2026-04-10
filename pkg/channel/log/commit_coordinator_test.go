package log

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestCommitCoordinatorBatchesMultipleGroupsIntoSinglePebbleSync(t *testing.T) {
	db, fs := openCountingCommitCoordinatorTestDB(t)
	coordinator := db.commitCoordinator()
	coordinator.flushWindow = 5 * time.Millisecond

	before := fs.syncCount.Load()

	errCh := make(chan error, 2)
	start := make(chan struct{})
	for i, channelKey := range []isr.ChannelKey{"group-1", "group-2"} {
		go func(channelKey isr.ChannelKey, index int) {
			<-start
			errCh <- coordinator.submit(commitRequest{
				channelKey: channelKey,
				build: func(batch *pebble.Batch) error {
					return batch.Set(
						encodeCheckpointKey(channelKey),
						encodeCheckpoint(isr.Checkpoint{Epoch: uint64(index + 1), HW: uint64(index + 1)}),
						pebble.NoSync,
					)
				},
			})
		}(channelKey, i)
	}
	close(start)

	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatalf("submit() error = %v", err)
		}
	}

	after := fs.syncCount.Load()
	if got := after - before; got != 1 {
		t.Fatalf("sync count delta = %d, want 1", got)
	}
}

func TestCommitCoordinatorDoesNotPublishBeforeSyncCompletes(t *testing.T) {
	db, fs := openBlockingSyncTestDB(t)
	coordinator := db.commitCoordinator()
	coordinator.flushWindow = 0

	published := make(chan struct{})
	done := make(chan error, 1)

	fs.enableNextSyncBlock()
	go func() {
		done <- coordinator.submit(commitRequest{
			channelKey: "group-1",
			build: func(batch *pebble.Batch) error {
				return batch.Set(
					encodeCheckpointKey("group-1"),
					encodeCheckpoint(isr.Checkpoint{Epoch: 3, HW: 1}),
					pebble.NoSync,
				)
			},
			publish: func() error {
				close(published)
				return nil
			},
		})
	}()

	select {
	case <-fs.syncStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for commit to reach durable sync")
	}

	select {
	case <-published:
		close(fs.syncContinue)
		<-done
		t.Fatal("publish callback ran before sync completed")
	case <-time.After(200 * time.Millisecond):
	}

	close(fs.syncContinue)
	if err := <-done; err != nil {
		t.Fatalf("submit() error = %v", err)
	}

	select {
	case <-published:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for publish callback after sync")
	}
}

func TestCommitCoordinatorFanoutsBatchFailureToAllWaiters(t *testing.T) {
	db, _ := openCountingCommitCoordinatorTestDB(t)
	coordinator := db.commitCoordinator()
	coordinator.flushWindow = 5 * time.Millisecond
	coordinator.commit = func(batch *pebble.Batch) error {
		return errSyntheticSyncFailure
	}

	publishCalls := 0
	var publishMu sync.Mutex
	errCh := make(chan error, 2)
	start := make(chan struct{})
	for _, channelKey := range []isr.ChannelKey{"group-1", "group-2"} {
		go func(channelKey isr.ChannelKey) {
			<-start
			errCh <- coordinator.submit(commitRequest{
				channelKey: channelKey,
				build: func(batch *pebble.Batch) error {
					return batch.Set(
						encodeCheckpointKey(channelKey),
						encodeCheckpoint(isr.Checkpoint{Epoch: 3, HW: 1}),
						pebble.NoSync,
					)
				},
				publish: func() error {
					publishMu.Lock()
					publishCalls++
					publishMu.Unlock()
					return nil
				},
			})
		}(channelKey)
	}
	close(start)

	for range 2 {
		err := <-errCh
		if err == nil {
			t.Fatal("expected sync failure to fan out to all waiters")
		}
		if !errors.Is(err, errSyntheticSyncFailure) {
			t.Fatalf("submit() error = %v, want synthetic sync failure", err)
		}
	}

	publishMu.Lock()
	defer publishMu.Unlock()
	if publishCalls != 0 {
		t.Fatalf("publish callbacks = %d, want 0", publishCalls)
	}
}

func openCountingCommitCoordinatorTestDB(tb testing.TB) (*DB, *countingFS) {
	tb.Helper()

	fs := newCountingFS(vfs.NewMem())
	pdb, err := pebble.Open("test", &pebble.Options{FS: fs})
	if err != nil {
		tb.Fatalf("pebble.Open() error = %v", err)
	}

	db := &DB{
		db:     pdb,
		stores: make(map[ChannelKey]*Store),
	}
	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Fatalf("Close() error = %v", err)
		}
	})
	return db, fs
}

var errSyntheticSyncFailure = errors.New("synthetic sync failure")
