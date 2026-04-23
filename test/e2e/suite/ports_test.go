//go:build e2e

package suite

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReserveLoopbackPortsReturnsDistinctAddresses(t *testing.T) {
	ports := ReserveLoopbackPorts(t)

	require.NotEmpty(t, ports.ClusterAddr)
	require.NotEmpty(t, ports.GatewayAddr)
	require.NotEmpty(t, ports.APIAddr)
	require.NotEqual(t, ports.ClusterAddr, ports.GatewayAddr)
	require.NotEqual(t, ports.ClusterAddr, ports.APIAddr)
	require.NotEqual(t, ports.GatewayAddr, ports.APIAddr)
	require.Contains(t, ports.ClusterAddr, "127.0.0.1:")
	require.Contains(t, ports.GatewayAddr, "127.0.0.1:")
	require.Contains(t, ports.APIAddr, "127.0.0.1:")
}
