package app

import (
	"testing"
	"time"

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
}

func TestConfigValidateRejectsMismatchedGroupCount(t *testing.T) {
	cfg := validConfig()
	cfg.Cluster.GroupCount = 2

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

func TestConfigValidateRejectsDuplicateGroupIDs(t *testing.T) {
	cfg := validConfig()
	cfg.Cluster.Groups = []GroupConfig{
		{ID: 1, Peers: []uint64{1}},
		{ID: 1, Peers: []uint64{1}},
	}
	cfg.Cluster.GroupCount = 2

	require.Error(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigValidateRejectsNodeMissingFromClusterNodes(t *testing.T) {
	cfg := validConfig()
	cfg.Node.ID = 2

	require.Error(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigValidateRejectsNodeMissingFromGroupPeers(t *testing.T) {
	cfg := validConfig()
	cfg.Node.ID = 2
	cfg.Cluster.Nodes = []NodeConfigRef{
		{ID: 1, Addr: "127.0.0.1:7000"},
		{ID: 2, Addr: "127.0.0.1:7001"},
	}

	require.Error(t, cfg.ApplyDefaultsAndValidate())
}

func TestConfigGatewayDefaultsSessionOptions(t *testing.T) {
	cfg := validConfig()
	cfg.Gateway.DefaultSession = gateway.SessionOptions{}

	require.NoError(t, cfg.ApplyDefaultsAndValidate())
	require.Equal(t, gateway.DefaultSessionOptions(), cfg.Gateway.DefaultSession)
}

func TestConfigGatewayPreservesExplicitFalseCloseOnHandlerError(t *testing.T) {
	cfg := validConfig()
	cfg.Gateway.DefaultSession = gateway.SessionOptions{
		CloseOnHandlerErrorSet: true,
		CloseOnHandlerError:    false,
	}

	require.NoError(t, cfg.ApplyDefaultsAndValidate())
	require.False(t, cfg.Gateway.DefaultSession.CloseOnHandlerError)
	require.True(t, cfg.Gateway.DefaultSession.CloseOnHandlerErrorSet)
}

func validConfig() Config {
	return Config{
		Node: NodeConfig{
			ID:      1,
			Name:    "node-1",
			DataDir: "/tmp/wukong-node-1",
		},
		Cluster: ClusterConfig{
			ListenAddr:     "127.0.0.1:7000",
			Nodes:          []NodeConfigRef{{ID: 1, Addr: "127.0.0.1:7000"}},
			Groups:         []GroupConfig{{ID: 1, Peers: []uint64{1}}},
			ForwardTimeout: 5 * time.Second,
			PoolSize:       4,
			TickInterval:   100 * time.Millisecond,
			RaftWorkers:    2,
			ElectionTick:   10,
			HeartbeatTick:  1,
			DialTimeout:    5 * time.Second,
		},
		Gateway: GatewayConfig{
			Listeners: []gateway.ListenerOptions{
				binding.TCPWKProto("tcp-wkproto", "127.0.0.1:5100"),
			},
		},
	}
}
