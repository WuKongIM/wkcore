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
		"WK_CLUSTER_SLOT_COUNT=1",
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
	require.Empty(t, cfg.Cluster.Slots)
	require.Len(t, cfg.Gateway.Listeners, 1)
}

func TestLoadConfigUsesBuiltInDefaultsWhenOptionalConfKeysAreMissing(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_SLOT_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
	)

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, "0.0.0.0:5001", cfg.API.ListenAddr)
	require.Len(t, cfg.Gateway.Listeners, 2)
	require.True(t, cfg.Observability.MetricsEnabled)
	require.True(t, cfg.Observability.HealthDetailEnabled)
	require.False(t, cfg.Observability.HealthDebugEnabled)
}

func TestLoadConfigParsesObservabilityFlags(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_SLOT_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
		"WK_METRICS_ENABLE=false",
		"WK_HEALTH_DETAIL_ENABLE=false",
		"WK_HEALTH_DEBUG_ENABLE=true",
	)

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.False(t, cfg.Observability.MetricsEnabled)
	require.False(t, cfg.Observability.HealthDetailEnabled)
	require.True(t, cfg.Observability.HealthDebugEnabled)
}

func TestLoadConfigParsesChannelBootstrapDefaultMinISR(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_SLOT_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
	)
	t.Setenv("WK_CLUSTER_CHANNEL_BOOTSTRAP_DEFAULT_MIN_ISR", "3")

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, 3, cfg.Cluster.ChannelBootstrapDefaultMinISR)
}

func TestLoadConfigRejectsExplicitZeroChannelBootstrapDefaultMinISR(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_SLOT_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
	)
	t.Setenv("WK_CLUSTER_CHANNEL_BOOTSTRAP_DEFAULT_MIN_ISR", "0")

	_, err := loadConfig(configPath)
	require.ErrorContains(t, err, "channel bootstrap default min isr")
}

func TestLoadConfigRejectsExplicitNegativeChannelBootstrapDefaultMinISR(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_SLOT_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
	)
	t.Setenv("WK_CLUSTER_CHANNEL_BOOTSTRAP_DEFAULT_MIN_ISR", "-1")

	_, err := loadConfig(configPath)
	require.ErrorContains(t, err, "channel bootstrap default min isr")
}

func TestLoadConfigPrefersEnvironmentVariablesOverConfValues(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_SLOT_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
		"WK_API_LISTEN_ADDR=127.0.0.1:8080",
	)
	t.Setenv("WK_API_LISTEN_ADDR", "127.0.0.1:9090")

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:9090", cfg.API.ListenAddr)
}

func TestLoadConfigParsesExternalRouteAddresses(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_SLOT_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
		"WK_EXTERNAL_TCPADDR=im.example.com:15100",
		"WK_EXTERNAL_WSADDR=ws://im.example.com:15200",
		"WK_EXTERNAL_WSSADDR=wss://im.example.com:15300",
	)

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, "im.example.com:15100", cfg.API.ExternalTCPAddr)
	require.Equal(t, "ws://im.example.com:15200", cfg.API.ExternalWSAddr)
	require.Equal(t, "wss://im.example.com:15300", cfg.API.ExternalWSSAddr)
}

func TestLoadConfigUsesDefaultSearchPathsWhenFlagPathIsEmpty(t *testing.T) {
	dir := t.TempDir()
	confDir := filepath.Join(dir, "conf")
	require.NoError(t, os.MkdirAll(confDir, 0o755))
	writeConf(t, confDir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_SLOT_COUNT=1",
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
	t.Setenv("WK_CLUSTER_SLOT_COUNT", "1")
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
		"WK_CLUSTER_SLOT_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
		`WK_CLUSTER_GROUPS=[{"id":1,"peers":[1]}]`,
	)

	_, err := loadConfig(configPath)
	require.ErrorContains(t, err, "WK_CLUSTER_GROUPS")
}

func TestLoadConfigRejectsLegacyClusterGroupCountKey(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_GROUP_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
	)

	_, err := loadConfig(configPath)
	require.ErrorContains(t, err, "WK_CLUSTER_GROUP_COUNT")
}

func TestLoadConfigRejectsLegacyClusterGroupReplicaNKey(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_SLOT_COUNT=1",
		"WK_CLUSTER_GROUP_REPLICA_N=3",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
	)

	_, err := loadConfig(configPath)
	require.ErrorContains(t, err, "WK_CLUSTER_GROUP_REPLICA_N")
}

func TestLoadConfigParsesDataPlaneRPCTimeoutFromConf(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_DATA_PLANE_RPC_TIMEOUT=250ms",
		"WK_CLUSTER_SLOT_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
	)

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, 250*time.Millisecond, cfg.Cluster.DataPlaneRPCTimeout)
}

func TestLoadConfigParsesClusterTimeoutOverridesFromConf(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_SLOT_COUNT=1",
		"WK_CLUSTER_CONTROLLER_OBSERVATION_INTERVAL=350ms",
		"WK_CLUSTER_CONTROLLER_REQUEST_TIMEOUT=3s",
		"WK_CLUSTER_CONTROLLER_LEADER_WAIT_TIMEOUT=9s",
		"WK_CLUSTER_FORWARD_RETRY_BUDGET=600ms",
		"WK_CLUSTER_MANAGED_SLOT_LEADER_WAIT_TIMEOUT=6s",
		"WK_CLUSTER_MANAGED_SLOT_CATCH_UP_TIMEOUT=7s",
		"WK_CLUSTER_MANAGED_SLOT_LEADER_MOVE_TIMEOUT=8s",
		"WK_CLUSTER_CONFIG_CHANGE_RETRY_BUDGET=700ms",
		"WK_CLUSTER_LEADER_TRANSFER_RETRY_BUDGET=800ms",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
	)

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, 350*time.Millisecond, cfg.Cluster.Timeouts.ControllerObservation)
	require.Equal(t, 3*time.Second, cfg.Cluster.Timeouts.ControllerRequest)
	require.Equal(t, 9*time.Second, cfg.Cluster.Timeouts.ControllerLeaderWait)
	require.Equal(t, 600*time.Millisecond, cfg.Cluster.Timeouts.ForwardRetryBudget)
	require.Equal(t, 6*time.Second, cfg.Cluster.Timeouts.ManagedSlotLeaderWait)
	require.Equal(t, 7*time.Second, cfg.Cluster.Timeouts.ManagedSlotCatchUp)
	require.Equal(t, 8*time.Second, cfg.Cluster.Timeouts.ManagedSlotLeaderMove)
	require.Equal(t, 700*time.Millisecond, cfg.Cluster.Timeouts.ConfigChangeRetryBudget)
	require.Equal(t, 800*time.Millisecond, cfg.Cluster.Timeouts.LeaderTransferRetryBudget)
}

func TestLoadConfigParsesObservationCadence(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_SLOT_COUNT=1",
		"WK_CLUSTER_OBSERVATION_HEARTBEAT_INTERVAL=2s",
		"WK_CLUSTER_OBSERVATION_RUNTIME_SCAN_INTERVAL=1s",
		"WK_CLUSTER_OBSERVATION_RUNTIME_FLUSH_DEBOUNCE=150ms",
		"WK_CLUSTER_OBSERVATION_RUNTIME_FULL_SYNC_INTERVAL=60s",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
	)

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, 2*time.Second, cfg.Cluster.Timeouts.ObservationHeartbeatInterval)
	require.Equal(t, time.Second, cfg.Cluster.Timeouts.ObservationRuntimeScanInterval)
	require.Equal(t, 150*time.Millisecond, cfg.Cluster.Timeouts.ObservationRuntimeFlushDebounce)
	require.Equal(t, 60*time.Second, cfg.Cluster.Timeouts.ObservationRuntimeFullSyncInterval)
}

func TestBuildAppConfigParsesAutomaticSlotManagementKeys(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_STORAGE_CONTROLLER_META_PATH="+filepath.Join(dir, "controller-meta"),
		"WK_STORAGE_CONTROLLER_RAFT_PATH="+filepath.Join(dir, "controller-raft"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_SLOT_COUNT=1",
		"WK_CLUSTER_CONTROLLER_REPLICA_N=3",
		"WK_CLUSTER_SLOT_REPLICA_N=3",
		`WK_CLUSTER_NODES=[{"id":3,"addr":"127.0.0.1:7002"},{"id":1,"addr":"127.0.0.1:7000"},{"id":2,"addr":"127.0.0.1:7001"}]`,
	)

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, 3, cfg.Cluster.ControllerReplicaN)
	require.Equal(t, 3, cfg.Cluster.SlotReplicaN)
	require.Equal(t, filepath.Join(dir, "controller-meta"), cfg.Storage.ControllerMetaPath)
	require.Equal(t, filepath.Join(dir, "controller-raft"), cfg.Storage.ControllerRaftPath)
	require.Empty(t, cfg.Cluster.Slots)
}

func TestLoadConfigParsesHashSlotAndInitialSlotCounts(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_HASH_SLOT_COUNT=256",
		"WK_CLUSTER_INITIAL_SLOT_COUNT=4",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
	)

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, uint16(256), cfg.Cluster.HashSlotCount)
	require.Equal(t, uint32(4), cfg.Cluster.InitialSlotCount)
}

func TestLoadConfigParsesGatewayAsyncSendDispatchFromConf(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_SLOT_COUNT=1",
		"WK_GATEWAY_DEFAULT_SESSION_ASYNC_SEND_DISPATCH=true",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
	)

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.True(t, cfg.Gateway.DefaultSession.AsyncSendDispatch)
}

func TestLoadConfigParsesLogSettingsFromConf(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_SLOT_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
		"WK_LOG_LEVEL=debug",
		"WK_LOG_DIR="+filepath.Join(dir, "logs"),
		"WK_LOG_MAX_SIZE=64",
		"WK_LOG_MAX_AGE=7",
		"WK_LOG_MAX_BACKUPS=3",
		"WK_LOG_COMPRESS=false",
		"WK_LOG_CONSOLE=false",
		"WK_LOG_FORMAT=json",
	)

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, "debug", cfg.Log.Level)
	require.Equal(t, filepath.Join(dir, "logs"), cfg.Log.Dir)
	require.Equal(t, 64, cfg.Log.MaxSize)
	require.Equal(t, 7, cfg.Log.MaxAge)
	require.Equal(t, 3, cfg.Log.MaxBackups)
	require.False(t, cfg.Log.Compress)
	require.False(t, cfg.Log.Console)
	require.Equal(t, "json", cfg.Log.Format)
}

func TestLoadConfigUsesLogDefaultsWhenUnset(t *testing.T) {
	dir := t.TempDir()
	configPath := writeConf(t, dir, "wukongim.conf",
		"WK_NODE_ID=1",
		"WK_NODE_DATA_DIR="+filepath.Join(dir, "node-1"),
		"WK_CLUSTER_LISTEN_ADDR=127.0.0.1:7000",
		"WK_CLUSTER_SLOT_COUNT=1",
		`WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:7000"}]`,
	)

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)
	require.Equal(t, "info", cfg.Log.Level)
	require.Equal(t, "./logs", cfg.Log.Dir)
	require.Equal(t, 100, cfg.Log.MaxSize)
	require.Equal(t, 30, cfg.Log.MaxAge)
	require.Equal(t, 10, cfg.Log.MaxBackups)
	require.True(t, cfg.Log.Compress)
	require.True(t, cfg.Log.Console)
	require.Equal(t, "console", cfg.Log.Format)
}
