package app

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raftstore"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestNewBuildsDBClusterStoreServiceAndGateway(t *testing.T) {
	cfg := testConfig(t)

	app, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, app.RaftDB().Close())
		require.NoError(t, app.DB().Close())
	})

	require.NotNil(t, app.DB())
	require.NotNil(t, app.RaftDB())
	require.NotNil(t, app.Cluster())
	require.NotNil(t, app.Store())
	require.NotNil(t, app.Service())
	require.NotNil(t, app.Gateway())
}

func TestNewReturnsConfigErrorsBeforeOpeningResources(t *testing.T) {
	cfg := testConfig(t)
	cfg.Node.ID = 0

	_, err := New(cfg)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, dbErr := os.Stat(cfg.Storage.DBPath)
	require.ErrorIs(t, dbErr, os.ErrNotExist)

	_, raftErr := os.Stat(cfg.Storage.RaftPath)
	require.ErrorIs(t, raftErr, os.ErrNotExist)
}

func TestAccessorsExposeBuiltRuntime(t *testing.T) {
	cfg := testConfig(t)

	app, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, app.RaftDB().Close())
		require.NoError(t, app.DB().Close())
	})

	require.Same(t, app.db, app.DB())
	require.Same(t, app.raftDB, app.RaftDB())
	require.Same(t, app.cluster, app.Cluster())
	require.Same(t, app.store, app.Store())
	require.Same(t, app.service, app.Service())
	require.Same(t, app.gateway, app.Gateway())
}

func TestNewClosesOpenedStoresWhenGatewayBuildFails(t *testing.T) {
	cfg := testConfig(t)
	cfg.Gateway.Listeners = append(cfg.Gateway.Listeners, cfg.Gateway.Listeners[0])

	_, err := New(cfg)
	require.Error(t, err)

	reopenedDB, dbOpenErr := openWKDBForTest(cfg.Storage.DBPath)
	require.NoError(t, dbOpenErr)
	require.NoError(t, reopenedDB.Close())

	reopenedRaft, raftOpenErr := openRaftDBForTest(cfg.Storage.RaftPath)
	require.NoError(t, raftOpenErr)
	require.NoError(t, reopenedRaft.Close())
}

func testConfig(t *testing.T) Config {
	t.Helper()

	cfg := validConfig()
	cfg.Node.DataDir = t.TempDir()
	cfg.Storage = StorageConfig{
		DBPath:   filepath.Join(cfg.Node.DataDir, "data"),
		RaftPath: filepath.Join(cfg.Node.DataDir, "raft"),
	}
	cfg.Gateway.Listeners[0].Address = "127.0.0.1:0"
	return cfg
}

func openWKDBForTest(path string) (interface{ Close() error }, error) {
	return wkdb.Open(path)
}

func openRaftDBForTest(path string) (interface{ Close() error }, error) {
	return raftstore.Open(path)
}
