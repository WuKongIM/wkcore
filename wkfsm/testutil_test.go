package wkfsm

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/WuKongIM/wraft/multiraft"
	"github.com/WuKongIM/wraft/raftstore"
	"github.com/WuKongIM/wraft/wkdb"
)

const (
	testTickInterval = 10 * time.Millisecond
	testElectionTick = 10
	testWaitTimeout  = testTickInterval * time.Duration(testElectionTick*20)
	testPollInterval = testTickInterval
)

func openTestDB(t *testing.T) *wkdb.DB {
	t.Helper()

	return openTestDBAt(t, filepath.Join(t.TempDir(), "db"))
}

func openTestDBAt(t *testing.T, path string) *wkdb.DB {
	t.Helper()

	db, err := wkdb.Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		defer func() {
			_ = recover()
		}()
		if err := db.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return db
}

func openTestRaftDBAt(t *testing.T, path string) *raftstore.DB {
	t.Helper()

	db, err := raftstore.Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		defer func() {
			_ = recover()
		}()
		if err := db.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return db
}

func newStartedRuntime(t *testing.T) *multiraft.Runtime {
	t.Helper()

	rt, err := multiraft.New(multiraft.Options{
		NodeID:       1,
		TickInterval: testTickInterval,
		Workers:      1,
		Transport:    fakeTransport{},
		Raft: multiraft.RaftOptions{
			ElectionTick:  testElectionTick,
			HeartbeatTick: 1,
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return rt
}

type fakeTransport struct{}

func (fakeTransport) Send(ctx context.Context, batch []multiraft.Envelope) error {
	return nil
}

func waitForCondition(t *testing.T, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(testWaitTimeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(testPollInterval)
	}
	t.Fatal("condition not satisfied before timeout")
}
