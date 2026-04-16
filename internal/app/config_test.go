package app

import (
	"testing"
	"time"

	accessapi "github.com/WuKongIM/WuKongIM/internal/access/api"
	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
	"github.com/stretchr/testify/require"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/binding"
)

func TestConfigValidateRequiresNodeAndClusterIdentity(t *testing.T) {
	t.Run("missing node id", func(t *testing.T) {
		cfg := validConfig()
		cfg.Node.ID = 0

		require.Error(t, cfg.ApplyDefaultsAndValidate())
	})

	t.Run("missing node data dir", func(t *testing.T) {
		cfg := validConfig()
		cfg.Node.DataDir = ""

		require.Error(t, cfg.ApplyDefaultsAndValidate())
	})

	t.Run("missing cluster listen addr", func(t *testing.T) {
		cfg := validConfig()
		cfg.Cluster.ListenAddr = ""

		require.Error(t, cfg.ApplyDefaultsAndValidate())
	})
}

func TestConfigApplyDefaultsDerivesStoragePathsFromDataDir(t *testing.T) {
	cfg := validConfig()
	cfg.Storage = StorageConfig{}

	require.NoError(t, cfg.ApplyDefaultsAndValidate())
	require.Equal(t, "/tmp/wukong-node-1/data", cfg.Storage.DBPath)
	require.Equal(t, "/tmp/wukong-node-1/raft", cfg.Storage.RaftPath)
	require.Equal(t, "/tmp/wukong-node-1/channellog", cfg.Storage.ChannelLogPath)
	require.Equal(t, "/tmp/wukong-node-1/controller-meta", cfg.Storage.ControllerMetaPath)
	require.Equal(t, "/tmp/wukong-node-1/controller-raft", cfg.Storage.ControllerRaftPath)
}

func TestConfigRejectsNodeIDSnowflakeOverflow(t *testing.T) {
	cfg := validConfig()
	cfg.Node.ID = 1024

	require.Error(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigValidateRejectsZeroSlotCount(t *testing.T) {
	cfg := validConfig()
	cfg.Cluster.SlotCount = 0

	require.Error(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigValidateRejectsHashSlotCountBelowInitialSlotCount(t *testing.T) {
	cfg := validConfig()
	cfg.Cluster.SlotCount = 0
	cfg.Cluster.InitialSlotCount = 4
	cfg.Cluster.HashSlotCount = 3

	require.Error(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigValidateRejectsMismatchedLegacyAndInitialSlotCount(t *testing.T) {
	cfg := validConfig()
	cfg.Cluster.SlotCount = 2
	cfg.Cluster.InitialSlotCount = 3
	cfg.Cluster.HashSlotCount = 3

	require.Error(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigValidateRejectsStaticClusterSlots(t *testing.T) {
	cfg := validConfig()
	cfg.Cluster.Slots = []SlotConfig{{ID: 1, Peers: []uint64{1}}}

	require.Error(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigValidateAllowsNilStaticSlotsWithExplicitSlotCount(t *testing.T) {
	cfg := validConfig()
	cfg.Cluster.Slots = nil
	cfg.Cluster.SlotCount = 1

	require.NoError(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigValidateRejectsInvalidControllerReplicaN(t *testing.T) {
	cfg := validConfig()
	cfg.Cluster.ControllerReplicaN = 4
	cfg.Cluster.SlotReplicaN = 3
	cfg.Cluster.Nodes = []NodeConfigRef{
		{ID: 3, Addr: "127.0.0.1:7002"},
		{ID: 1, Addr: "127.0.0.1:7000"},
		{ID: 2, Addr: "127.0.0.1:7001"},
	}

	require.Error(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigValidateRejectsSharedStoragePaths(t *testing.T) {
	cfg := validConfig()
	cfg.Storage.DBPath = "/tmp/wukong-node-1/shared"
	cfg.Storage.RaftPath = "/tmp/wukong-node-1/shared"

	require.Error(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigValidateRejectsAliasedSharedStoragePaths(t *testing.T) {
	cfg := validConfig()
	cfg.Storage.DBPath = "/tmp/wukong-node-1/data"
	cfg.Storage.RaftPath = "/tmp/wukong-node-1/data/"

	require.Error(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigValidateRejectsSharedChannelLogPath(t *testing.T) {
	cfg := validConfig()
	cfg.Storage.ChannelLogPath = "/tmp/wukong-node-1/data"

	require.Error(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigValidateRejectsDuplicateClusterNodeIDs(t *testing.T) {
	cfg := validConfig()
	cfg.Cluster.Nodes = []NodeConfigRef{
		{ID: 1, Addr: "127.0.0.1:7000"},
		{ID: 1, Addr: "127.0.0.1:7001"},
	}

	require.Error(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigValidateRejectsNodeMissingFromClusterNodes(t *testing.T) {
	cfg := validConfig()
	cfg.Node.ID = 2

	require.Error(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigValidateRejectsLocalNodeMissingFromClusterNodes(t *testing.T) {
	cfg := validConfig()
	cfg.Node.ID = 9
	cfg.Cluster.ControllerReplicaN = 3
	cfg.Cluster.SlotReplicaN = 3

	require.Error(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigGatewayDefaultsSessionOptions(t *testing.T) {
	cfg := validConfig()
	cfg.Gateway.DefaultSession = gateway.SessionOptions{}

	require.NoError(t, cfg.ApplyDefaultsAndValidate())
	require.NotNil(t, cfg.Gateway.DefaultSession.CloseOnHandlerError)
	require.True(t, *cfg.Gateway.DefaultSession.CloseOnHandlerError)
}

func TestConfigGatewayPreservesExplicitFalseCloseOnHandlerError(t *testing.T) {
	cfg := validConfig()
	cfg.Gateway.DefaultSession = gateway.SessionOptions{
		CloseOnHandlerError: boolPtr(false),
	}

	require.NoError(t, cfg.ApplyDefaultsAndValidate())
	require.NotNil(t, cfg.Gateway.DefaultSession.CloseOnHandlerError)
	require.False(t, *cfg.Gateway.DefaultSession.CloseOnHandlerError)
}

func TestConfigValidateRejectsTokenAuthWithoutHooks(t *testing.T) {
	cfg := validConfig()
	cfg.Gateway.TokenAuthOn = true

	require.Error(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigAllowsDisabledAPIWhenListenAddrEmpty(t *testing.T) {
	cfg := validConfig()
	cfg.API = APIConfig{}

	require.NoError(t, cfg.ApplyDefaultsAndValidate())
	require.Equal(t, "", cfg.API.ListenAddr)
}

func TestLegacyRouteAddressesPreferExplicitExternalConfig(t *testing.T) {
	cfg := validConfig()
	cfg.Gateway.Listeners = []gateway.ListenerOptions{
		binding.TCPWKProto("tcp-wkproto", "127.0.0.1:5100"),
		binding.WSJSONRPC("ws-jsonrpc", "127.0.0.1:5200"),
	}
	cfg.API.ExternalTCPAddr = "im.example.com:15100"
	cfg.API.ExternalWSSAddr = "wss://im.example.com:15300"

	external, intranet := legacyRouteAddresses(cfg.API, cfg.Gateway.Listeners)

	require.Equal(t, accessapi.LegacyRouteAddresses{
		TCPAddr: "im.example.com:15100",
		WSAddr:  "ws://127.0.0.1:5200",
		WSSAddr: "wss://im.example.com:15300",
	}, external)
	require.Equal(t, accessapi.LegacyRouteAddresses{
		TCPAddr: "127.0.0.1:5100",
	}, intranet)
}

func TestConfigPreservesExplicitDataPlaneRPCTimeout(t *testing.T) {
	cfg := validConfig()
	cfg.Cluster.DataPlaneRPCTimeout = 250 * time.Millisecond

	require.NoError(t, cfg.ApplyDefaultsAndValidate())
	require.Equal(t, 250*time.Millisecond, cfg.Cluster.DataPlaneRPCTimeout)
}

func TestClusterRuntimeConfigIncludesTimeoutOverrides(t *testing.T) {
	cfg := validConfig()
	cfg.Cluster.Timeouts = raftcluster.Timeouts{
		ControllerObservation:     350 * time.Millisecond,
		ControllerRequest:         3 * time.Second,
		ControllerLeaderWait:      9 * time.Second,
		ForwardRetryBudget:        600 * time.Millisecond,
		ManagedSlotLeaderWait:     6 * time.Second,
		ManagedSlotCatchUp:        7 * time.Second,
		ManagedSlotLeaderMove:     8 * time.Second,
		ConfigChangeRetryBudget:   700 * time.Millisecond,
		LeaderTransferRetryBudget: 800 * time.Millisecond,
	}

	runtimeCfg := cfg.Cluster.runtimeConfig(cfg.Storage, nil, nil, cfg.Node.ID, nil)

	require.Equal(t, cfg.Cluster.Timeouts, runtimeCfg.Timeouts)
}

func validConfig() Config {
	return Config{
		Node: NodeConfig{
			ID:      1,
			Name:    "node-1",
			DataDir: "/tmp/wukong-node-1",
		},
		Cluster: ClusterConfig{
			ListenAddr:         "127.0.0.1:7000",
			SlotCount:          1,
			Nodes:              []NodeConfigRef{{ID: 1, Addr: "127.0.0.1:7000"}},
			ControllerReplicaN: 1,
			SlotReplicaN:       1,
			ForwardTimeout:     5 * time.Second,
			PoolSize:           4,
			TickInterval:       100 * time.Millisecond,
			RaftWorkers:        2,
			ElectionTick:       10,
			HeartbeatTick:      1,
			DialTimeout:        5 * time.Second,
		},
		API: APIConfig{},
		Gateway: GatewayConfig{
			Listeners: []gateway.ListenerOptions{
				binding.TCPWKProto("tcp-wkproto", "127.0.0.1:5100"),
			},
		},
	}
}

func boolPtr(v bool) *bool { return &v }
