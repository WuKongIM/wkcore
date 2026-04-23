//go:build e2e

package suite

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Workspace owns the per-test filesystem layout for e2e child processes.
type Workspace struct {
	RootDir string
}

// Suite owns one test-scoped e2e environment.
type Suite struct {
	t          *testing.T
	binaryPath string
	workspace  Workspace
}

// StartedNode describes one started process and its external addresses.
type StartedNode struct {
	Spec    NodeSpec
	Process *NodeProcess
}

// StartedCluster describes one started three-node cluster and its last observations.
type StartedCluster struct {
	Nodes          []StartedNode
	lastReadyz     map[uint64]HTTPObservation
	lastSlotBodies map[uint32]string
}

// NewWorkspace creates a temp workspace with a default node-1 tree for phase 1.
func NewWorkspace(t *testing.T) Workspace {
	t.Helper()

	rootDir := t.TempDir()
	workspace := Workspace{RootDir: rootDir}
	require.NoError(t, workspace.ensureNodeDirs(1))
	return workspace
}

// New creates a phase-1 suite using the prebuilt production binary.
func New(t *testing.T, binaryPath string) *Suite {
	t.Helper()

	return &Suite{
		t:          t,
		binaryPath: binaryPath,
		workspace:  NewWorkspace(t),
	}
}

// StartSingleNodeCluster starts one real child process and waits for WKProto readiness.
func (s *Suite) StartSingleNodeCluster() *StartedNode {
	s.t.Helper()

	ports := ReserveLoopbackPorts(s.t)
	spec := s.singleNodeSpec(1, ports)
	require.NoError(s.t, s.workspace.ensureNodeDirs(spec.ID))
	require.NoError(s.t, os.WriteFile(spec.ConfigPath, []byte(RenderSingleNodeConfig(spec)), 0o644))

	process := &NodeProcess{
		Spec:       spec,
		BinaryPath: s.binaryPath,
	}
	require.NoError(s.t, process.Start())
	s.t.Cleanup(func() {
		require.NoError(s.t, process.Stop())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(s.t, WaitWKProtoReady(ctx, spec.GatewayAddr), process.DumpDiagnostics())

	return &StartedNode{Spec: spec, Process: process}
}

// StartThreeNodeCluster starts three real child processes and returns a cluster handle.
func (s *Suite) StartThreeNodeCluster() *StartedCluster {
	s.t.Helper()

	ports := []PortSet{
		ReserveLoopbackPorts(s.t),
		ReserveLoopbackPorts(s.t),
		ReserveLoopbackPorts(s.t),
	}
	specs := make([]NodeSpec, 0, len(ports))
	for i, portSet := range ports {
		nodeID := uint64(i + 1)
		spec := s.singleNodeSpec(nodeID, portSet)
		require.NoError(s.t, s.workspace.ensureNodeDirs(nodeID))
		specs = append(specs, spec)
	}

	for _, spec := range specs {
		require.NoError(s.t, os.WriteFile(spec.ConfigPath, []byte(RenderClusterConfig(spec, specs)), 0o644))
	}

	cluster := &StartedCluster{
		Nodes:          make([]StartedNode, 0, len(specs)),
		lastReadyz:     make(map[uint64]HTTPObservation, len(specs)),
		lastSlotBodies: make(map[uint32]string),
	}
	for _, spec := range specs {
		process := &NodeProcess{Spec: spec, BinaryPath: s.binaryPath}
		require.NoError(s.t, process.Start())
		cluster.Nodes = append(cluster.Nodes, StartedNode{Spec: spec, Process: process})
	}

	s.t.Cleanup(func() {
		for i := len(cluster.Nodes) - 1; i >= 0; i-- {
			require.NoError(s.t, cluster.Nodes[i].Process.Stop())
		}
	})

	return cluster
}

// WaitClusterReady waits until every node satisfies the node-ready contract.
func (c *StartedCluster) WaitClusterReady(ctx context.Context) error {
	if c == nil {
		return fmt.Errorf("started cluster is nil")
	}
	for _, node := range c.Nodes {
		observation, err := waitNodeReadyDetailed(ctx, node)
		c.lastReadyz[node.Spec.ID] = observation
		if err != nil {
			return fmt.Errorf("node %d not ready: %w", node.Spec.ID, err)
		}
	}
	return nil
}

// DumpDiagnostics returns a cluster-scoped snapshot of the last observations and node artifacts.
func (c *StartedCluster) DumpDiagnostics() string {
	if c == nil {
		return "cluster: <nil>\n"
	}

	var b strings.Builder
	for _, node := range c.Nodes {
		fmt.Fprintf(&b, "node %d diagnostics:\n", node.Spec.ID)
		if observation, ok := c.lastReadyz[node.Spec.ID]; ok {
			fmt.Fprintf(&b, "readyz: status=%d body=%s\n", observation.StatusCode, observation.Body)
		}
		process := node.Process
		if process == nil {
			process = &NodeProcess{Spec: node.Spec}
		}
		b.WriteString(process.DumpDiagnostics())
	}
	for slotID, body := range c.lastSlotBodies {
		fmt.Fprintf(&b, "slot %d body: %s\n", slotID, body)
	}
	return b.String()
}

// Node looks up one node handle by node ID.
func (c *StartedCluster) Node(nodeID uint64) (*StartedNode, bool) {
	if c == nil {
		return nil, false
	}
	for i := range c.Nodes {
		if c.Nodes[i].Spec.ID == nodeID {
			return &c.Nodes[i], true
		}
	}
	return nil, false
}

// MustNode looks up one node handle by node ID and panics when missing.
func (c *StartedCluster) MustNode(nodeID uint64) *StartedNode {
	node, ok := c.Node(nodeID)
	if !ok {
		panic(fmt.Sprintf("node %d not found", nodeID))
	}
	return node
}

// GatewayAddr returns the public WKProto listen address for the started node.
func (n *StartedNode) GatewayAddr() string {
	return n.Spec.GatewayAddr
}

func (s *Suite) singleNodeSpec(nodeID uint64, ports PortSet) NodeSpec {
	return NodeSpec{
		ID:          nodeID,
		Name:        "node-" + strconv.FormatUint(nodeID, 10),
		RootDir:     s.workspace.NodeRootDir(nodeID),
		DataDir:     s.workspace.NodeDataDir(nodeID),
		ConfigPath:  s.workspace.NodeConfigPath(nodeID),
		StdoutPath:  s.workspace.NodeStdoutPath(nodeID),
		StderrPath:  s.workspace.NodeStderrPath(nodeID),
		ClusterAddr: ports.ClusterAddr,
		GatewayAddr: ports.GatewayAddr,
		APIAddr:     ports.APIAddr,
		ManagerAddr: ports.ManagerAddr,
		LogDir:      s.workspace.NodeLogDir(nodeID),
	}
}

func (w Workspace) ensureNodeDirs(nodeID uint64) error {
	if err := os.MkdirAll(w.NodeDataDir(nodeID), 0o755); err != nil {
		return err
	}
	return os.MkdirAll(w.NodeLogDir(nodeID), 0o755)
}

// NodeRootDir returns the root directory for one node.
func (w Workspace) NodeRootDir(nodeID uint64) string {
	return filepath.Join(w.RootDir, nodeDirName(nodeID))
}

// NodeDataDir returns the data directory for one node.
func (w Workspace) NodeDataDir(nodeID uint64) string {
	return filepath.Join(w.NodeRootDir(nodeID), "data")
}

// NodeLogDir returns the log directory for one node.
func (w Workspace) NodeLogDir(nodeID uint64) string {
	return filepath.Join(w.NodeRootDir(nodeID), "logs")
}

// NodeConfigPath returns the config file path for one node.
func (w Workspace) NodeConfigPath(nodeID uint64) string {
	return filepath.Join(w.NodeRootDir(nodeID), "wukongim.conf")
}

// NodeStdoutPath returns the stdout log path for one node.
func (w Workspace) NodeStdoutPath(nodeID uint64) string {
	return filepath.Join(w.NodeRootDir(nodeID), "stdout.log")
}

// NodeStderrPath returns the stderr log path for one node.
func (w Workspace) NodeStderrPath(nodeID uint64) string {
	return filepath.Join(w.NodeRootDir(nodeID), "stderr.log")
}

func nodeDirName(nodeID uint64) string {
	return "node-" + strconv.FormatUint(nodeID, 10)
}
