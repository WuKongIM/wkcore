package log

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestStoreApplyFetchPublishesLEOOnlyAfterCoordinatorCommit(t *testing.T) {
	db, fs := openBlockingSyncTestDB(t)
	store := db.ForChannel(ChannelKey{ChannelID: "c1", ChannelType: 1})
	db.commitCoordinator().flushWindow = 0

	leo, err := store.leo()
	if err != nil {
		t.Fatalf("leo() warmup error = %v", err)
	}
	if leo != 0 {
		t.Fatalf("warmup leo = %d, want 0", leo)
	}

	state, err := db.StateStoreFactory().ForChannel(store.key)
	if err != nil {
		t.Fatalf("StateStoreFactory().ForChannel() error = %v", err)
	}
	checkpoints, err := newCheckpointBridge(store.isrCheckpointStore(), store, db, store.key, state, store.channelKey)
	if err != nil {
		t.Fatalf("newCheckpointBridge() error = %v", err)
	}

	fs.enableNextSyncBlock()
	applyDone := make(chan error, 1)
	go func() {
		_, err := checkpoints.StoreApplyFetch(isr.ApplyFetchStoreRequest{
			Records:    []isr.Record{mustStoredRecord(t, 1, "one")},
			Checkpoint: &isr.Checkpoint{Epoch: 3, HW: 1},
		})
		applyDone <- err
	}()

	select {
	case <-fs.syncStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for StoreApplyFetch() to reach durable commit")
	}

	store.mu.Lock()
	blockedLEO := store.cachedLEO
	store.mu.Unlock()
	if blockedLEO != 0 {
		close(fs.syncContinue)
		<-applyDone
		t.Fatalf("cached LEO during blocked coordinator commit = %d, want 0", blockedLEO)
	}

	close(fs.syncContinue)
	if err := <-applyDone; err != nil {
		t.Fatalf("StoreApplyFetch() error = %v", err)
	}

	leo, err = store.leo()
	if err != nil {
		t.Fatalf("leo() after commit error = %v", err)
	}
	if leo != 1 {
		t.Fatalf("leo() after commit = %d, want 1", leo)
	}
}

func TestISRBridgeLEODoesNotBlockWhileCoordinatorSyncIsInFlight(t *testing.T) {
	db, fs := openBlockingSyncTestDB(t)
	store := db.ForChannel(ChannelKey{ChannelID: "c1", ChannelType: 1})
	db.commitCoordinator().flushWindow = 0
	logBridge := store.isrLogStore()

	leo, err := store.leo()
	if err != nil {
		t.Fatalf("leo() warmup error = %v", err)
	}
	if leo != 0 {
		t.Fatalf("warmup leo = %d, want 0", leo)
	}

	state, err := db.StateStoreFactory().ForChannel(store.key)
	if err != nil {
		t.Fatalf("StateStoreFactory().ForChannel() error = %v", err)
	}
	checkpoints, err := newCheckpointBridge(store.isrCheckpointStore(), store, db, store.key, state, store.channelKey)
	if err != nil {
		t.Fatalf("newCheckpointBridge() error = %v", err)
	}

	fs.enableNextSyncBlock()
	applyDone := make(chan error, 1)
	go func() {
		_, err := checkpoints.StoreApplyFetch(isr.ApplyFetchStoreRequest{
			Records:    []isr.Record{mustStoredRecord(t, 1, "one")},
			Checkpoint: &isr.Checkpoint{Epoch: 3, HW: 1},
		})
		applyDone <- err
	}()

	select {
	case <-fs.syncStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for StoreApplyFetch() to reach durable commit")
	}

	leoDone := make(chan struct{})
	var blockedLEO uint64
	go func() {
		blockedLEO = logBridge.LEO()
		close(leoDone)
	}()

	select {
	case <-leoDone:
	case <-time.After(200 * time.Millisecond):
		close(fs.syncContinue)
		<-applyDone
		t.Fatal("bridge LEO() blocked while coordinator commit was in progress")
	}

	if blockedLEO != 0 {
		close(fs.syncContinue)
		<-applyDone
		t.Fatalf("bridge LEO() during blocked commit = %d, want 0", blockedLEO)
	}

	close(fs.syncContinue)
	if err := <-applyDone; err != nil {
		t.Fatalf("StoreApplyFetch() error = %v", err)
	}

	leo, err = store.leo()
	if err != nil {
		t.Fatalf("leo() after commit error = %v", err)
	}
	if leo != 1 {
		t.Fatalf("leo() after commit = %d, want 1", leo)
	}
}

func openBlockingSyncTestDB(tb testing.TB) (*DB, *blockingSyncFS) {
	tb.Helper()

	dir := tb.TempDir()
	fs := newBlockingSyncFS(vfs.Default)
	pdb, err := pebble.Open(dir, &pebble.Options{FS: fs})
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

type blockingSyncFS struct {
	vfs.FS

	enabled      atomic.Bool
	syncStarted  chan struct{}
	syncContinue chan struct{}
	once         sync.Once
}

func newBlockingSyncFS(base vfs.FS) *blockingSyncFS {
	return &blockingSyncFS{
		FS:           base,
		syncStarted:  make(chan struct{}),
		syncContinue: make(chan struct{}),
	}
}

func (fs *blockingSyncFS) enableNextSyncBlock() {
	fs.enabled.Store(true)
}

func (fs *blockingSyncFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := fs.FS.Create(name, category)
	if err != nil {
		return nil, err
	}
	return fs.wrap(file), nil
}

func (fs *blockingSyncFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := fs.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return fs.wrap(file), nil
}

func (fs *blockingSyncFS) OpenReadWrite(name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := fs.FS.OpenReadWrite(name, category, opts...)
	if err != nil {
		return nil, err
	}
	return fs.wrap(file), nil
}

func (fs *blockingSyncFS) OpenDir(name string) (vfs.File, error) {
	file, err := fs.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return fs.wrap(file), nil
}

func (fs *blockingSyncFS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := fs.FS.ReuseForWrite(oldname, newname, category)
	if err != nil {
		return nil, err
	}
	return fs.wrap(file), nil
}

func (fs *blockingSyncFS) wrap(file vfs.File) vfs.File {
	if file == nil {
		return nil
	}
	return &blockingSyncFile{File: file, fs: fs}
}

func (fs *blockingSyncFS) maybeBlockSync() {
	if !fs.enabled.Load() {
		return
	}
	fs.once.Do(func() {
		close(fs.syncStarted)
		<-fs.syncContinue
	})
}

type blockingSyncFile struct {
	vfs.File
	fs *blockingSyncFS
}

func (f *blockingSyncFile) Sync() error {
	f.fs.maybeBlockSync()
	return f.File.Sync()
}

func (f *blockingSyncFile) SyncTo(length int64) (bool, error) {
	f.fs.maybeBlockSync()
	return f.File.SyncTo(length)
}

func (f *blockingSyncFile) SyncData() error {
	f.fs.maybeBlockSync()
	return f.File.SyncData()
}
