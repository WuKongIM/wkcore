package wkfsm

import (
	"path/filepath"
	"testing"

	"github.com/WuKongIM/wraft/wkdb"
)

func openTestDB(t *testing.T) *wkdb.DB {
	t.Helper()

	db, err := wkdb.Open(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return db
}
