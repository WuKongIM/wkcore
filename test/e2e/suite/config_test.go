//go:build e2e

package suite

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderSingleNodeConfigUsesOfficialWKKeys(t *testing.T) {
	spec := NodeSpec{
		ID:          1,
		Name:        "node-1",
		DataDir:     "/tmp/node-1/data",
		ConfigPath:  "/tmp/node-1/wukongim.conf",
		ClusterAddr: "127.0.0.1:17000",
		GatewayAddr: "127.0.0.1:15100",
		APIAddr:     "127.0.0.1:18080",
	}

	cfg := RenderSingleNodeConfig(spec)
	require.Contains(t, cfg, "WK_NODE_ID=1")
	require.Contains(t, cfg, "WK_NODE_NAME=node-1")
	require.Contains(t, cfg, "WK_NODE_DATA_DIR=/tmp/node-1/data")
	require.Contains(t, cfg, "WK_CLUSTER_LISTEN_ADDR=127.0.0.1:17000")
	require.Contains(t, cfg, "WK_CLUSTER_SLOT_COUNT=1")
	require.Contains(t, cfg, `WK_CLUSTER_NODES=[{"id":1,"addr":"127.0.0.1:17000"}]`)
	require.Contains(t, cfg, `"address":"127.0.0.1:15100"`)
	require.Contains(t, cfg, `"transport":"stdnet"`)
	require.Contains(t, cfg, "WK_API_LISTEN_ADDR=127.0.0.1:18080")
}
