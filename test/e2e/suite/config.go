//go:build e2e

package suite

import (
	"encoding/json"
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
	ManagerAddr string
	LogDir      string
}

// RenderSingleNodeConfig renders a real wukongim.conf file for one-node clusters.
func RenderSingleNodeConfig(spec NodeSpec) string {
	return RenderClusterConfig(spec, []NodeSpec{spec})
}

// RenderClusterConfig renders one real wukongim.conf file for a managed cluster node.
func RenderClusterConfig(local NodeSpec, nodes []NodeSpec) string {
	lines := []string{
		fmt.Sprintf("WK_NODE_ID=%d", local.ID),
		fmt.Sprintf("WK_NODE_NAME=%s", local.Name),
		fmt.Sprintf("WK_NODE_DATA_DIR=%s", local.DataDir),
		fmt.Sprintf("WK_CLUSTER_LISTEN_ADDR=%s", local.ClusterAddr),
		"WK_CLUSTER_SLOT_COUNT=1",
		fmt.Sprintf("WK_CLUSTER_INITIAL_SLOT_COUNT=%d", 1),
		fmt.Sprintf("WK_CLUSTER_CONTROLLER_REPLICA_N=%d", len(nodes)),
		fmt.Sprintf("WK_CLUSTER_SLOT_REPLICA_N=%d", len(nodes)),
		fmt.Sprintf("WK_CLUSTER_NODES=%s", marshalClusterNodes(nodes)),
		fmt.Sprintf(`WK_GATEWAY_LISTENERS=[{"name":"tcp-wkproto","network":"tcp","address":"%s","transport":"stdnet","protocol":"wkproto"}]`, local.GatewayAddr),
	}
	if local.APIAddr != "" {
		lines = append(lines, fmt.Sprintf("WK_API_LISTEN_ADDR=%s", local.APIAddr))
	}
	if local.ManagerAddr != "" {
		lines = append(lines, fmt.Sprintf("WK_MANAGER_LISTEN_ADDR=%s", local.ManagerAddr))
		lines = append(lines, "WK_MANAGER_AUTH_ON=false")
	}
	if local.LogDir != "" {
		lines = append(lines, fmt.Sprintf("WK_LOG_DIR=%s", local.LogDir))
	}
	return strings.Join(lines, "\n") + "\n"
}

func marshalClusterNodes(nodes []NodeSpec) string {
	type clusterNode struct {
		ID   uint64 `json:"id"`
		Addr string `json:"addr"`
	}

	items := make([]clusterNode, 0, len(nodes))
	for _, node := range nodes {
		items = append(items, clusterNode{
			ID:   node.ID,
			Addr: node.ClusterAddr,
		})
	}

	data, err := json.Marshal(items)
	if err != nil {
		panic(fmt.Sprintf("marshal cluster nodes: %v", err))
	}
	return string(data)
}
