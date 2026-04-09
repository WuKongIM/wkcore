package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func writeConf(t *testing.T, dir, name string, lines ...string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	body := strings.Join(lines, "\n") + "\n"
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	return path
}

func chdirForTest(t *testing.T, dir string) {
	t.Helper()

	cwd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() {
		require.NoError(t, os.Chdir(cwd))
	})
}

func TestLoadConfigParsesConfFileIntoAppConfig(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "node-1")
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_NAME=node-1",
		"WK_NODE_DATA_DIR="+dataDir,
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_GROUP_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
		`WK_GATEWAY_LISTENERS=[{"name":"tcp-wkproto","network":"tcp","address":"127.0.0.1:5100","transport":"stdnet","protocol":"wkproto"}]`,
		"WK_API_LISTEN_ADDR=127.0.0.1:8080",
	)

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, uint64(1), cfg.Node.ID)
	require.Equal(t, "node-1", cfg.Node.Name)
	require.Equal(t, dataDir, cfg.Node.DataDir)
	require.Equal(t, "127.0.0.1:7000", cfg.Cluster.ListenAddr)
	require.Equal(t, "127.0.0.1:8080", cfg.API.ListenAddr)
	require.Len(t, cfg.Cluster.Nodes, 1)
	require.Empty(t, cfg.Cluster.Groups)
	require.Len(t, cfg.Gateway.Listeners, 1)
}

func TestLoadConfigUsesBuiltInDefaultsWhenOptionalConfKeysAreMissing(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_GROUP_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
	)

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, "0.0.0.0:5001", cfg.API.ListenAddr)
	require.Len(t, cfg.Gateway.Listeners, 2)
}

func TestLoadConfigPrefersEnvironmentVariablesOverConfValues(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_GROUP_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
		"WK_API_LISTEN_ADDR=127.0.0.1:8080",
	)
	t.Setenv("WK_API_LISTEN_ADDR", "127.0.0.1:9090")

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:9090", cfg.API.ListenAddr)
}

func TestLoadConfigUsesDefaultSearchPathsWhenFlagPathIsEmpty(t *testing.T) {
	dir := t.TempDir()
	confDir := filepath.Join(dir, "conf")
	require.NoError(t, os.MkdirAll(confDir, 0o755))
	writeConf(t, confDir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_GROUP_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
	)
	chdirForTest(t, dir)

	cfg, err := loadConfig("")
	require.NoError(t, err)
	require.Equal(t, uint64(1), cfg.Node.ID)
}

func TestLoadConfigAcceptsEnvironmentOnlyConfigurationWhenNoFileExists(t *testing.T) {
	dir := t.TempDir()
	chdirForTest(t, dir)

	t.Setenv("WK_NODE_ID", "1")
	t.Setenv("WK_NODE_DATA_DIR", filepath.Join(dir, "node-1"))
	t.Setenv("WK_CLUSTER_LISTEN_ADDR", "127.0.0.1:7000")
	t.Setenv("WK_CLUSTER_GROUP_COUNT", "1")
	t.Setenv("WK_CLUSTER_NODES", `[{"id":1,"addr":"127.0.0.1:7000"}]`)

	cfg, err := loadConfig("")
	require.NoError(t, err)
	require.Equal(t, uint64(1), cfg.Node.ID)
}

func TestLoadConfigReportsAttemptedDefaultPathsWhenConfigIsMissing(t *testing.T) {
	dir := t.TempDir()
	chdirForTest(t, dir)

	_, err := loadConfig("")
	require.ErrorContains(t, err, "./wukongim.conf")
	require.ErrorContains(t, err, "./conf/wukongim.conf")
	require.ErrorContains(t, err, "/etc/wukongim/wukongim.conf")
}

func TestLoadConfigRejectsLegacyClusterGroupsKey(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_GROUP_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
		`WK_CLUSTER_GROUPS=[{"id":1,"peers":[1]}]`,
	)

	_, err := loadConfig(configPath)
	require.ErrorContains(t, err, "WK_CLUSTER_GROUPS")
}

func TestLoadConfigParsesDataPlaneRPCTimeoutFromConf(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_DATA_PLANE_RPC_TIMEOUT=250ms",
		"WK_CLUSTER_GROUP_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
	)

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, 250*time.Millisecond, cfg.Cluster.DataPlaneRPCTimeout)
}

func TestBuildAppConfigParsesAutomaticGroupManagementKeys(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_STORAGE_CONTROLLER_META_PATH="+filepath.Join(dir, "controller-meta"),
		"WK_STORAGE_CONTROLLER_RAFT_PATH="+filepath.Join(dir, "controller-raft"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_GROUP_COUNT=1",
		"WK_CLUSTER_CONTROLLER_REPLICA_N=3",
		"WK_CLUSTER_GROUP_REPLICA_N=3",
		`WK_CLUSTER_NODES=[{"id":3,"addr":"127.0.0.1:7002"},{"id":1,"addr":"127.0.0.1:7000"},{"id":2,"addr":"127.0.0.1:7001"}]`,
	)

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, 3, cfg.Cluster.ControllerReplicaN)
	require.Equal(t, 3, cfg.Cluster.GroupReplicaN)
	require.Equal(t, filepath.Join(dir, "controller-meta"), cfg.Storage.ControllerMetaPath)
	require.Equal(t, filepath.Join(dir, "controller-raft"), cfg.Storage.ControllerRaftPath)
	require.Empty(t, cfg.Cluster.Groups)
}
