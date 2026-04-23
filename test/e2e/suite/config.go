//go:build e2e

package suite

import (
	"fmt"
	"strings"
)

// NodeSpec describes the external runtime inputs for one e2e node process.
type NodeSpec struct {
	ID          uint64
	Name        string
	RootDir     string
	DataDir     string
	ConfigPath  string
	StdoutPath  string
	StderrPath  string
	ClusterAddr string
	GatewayAddr string
	APIAddr     string
}

// RenderSingleNodeConfig renders a real wukongim.conf file for one-node clusters.
func RenderSingleNodeConfig(spec NodeSpec) string {
	lines := []string{
		fmt.Sprintf("WK_NODE_ID=%d", spec.ID),
		fmt.Sprintf("WK_NODE_NAME=%s", spec.Name),
		fmt.Sprintf("WK_NODE_DATA_DIR=%s", spec.DataDir),
		fmt.Sprintf("WK_CLUSTER_LISTEN_ADDR=%s", spec.ClusterAddr),
		"WK_CLUSTER_SLOT_COUNT=1",
		fmt.Sprintf(`WK_CLUSTER_NODES=[{"id":%d,"addr":"%s"}]`, spec.ID, spec.ClusterAddr),
		fmt.Sprintf(`WK_GATEWAY_LISTENERS=[{"name":"tcp-wkproto","network":"tcp","address":"%s","transport":"stdnet","protocol":"wkproto"}]`, spec.GatewayAddr),
	}
	if spec.APIAddr != "" {
		lines = append(lines, fmt.Sprintf("WK_API_LISTEN_ADDR=%s", spec.APIAddr))
	}
	return strings.Join(lines, "\n") + "\n"
}
