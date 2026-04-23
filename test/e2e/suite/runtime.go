//go:build e2e

package suite

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
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

// NewWorkspace creates a temp workspace with a default node-1 tree for phase 1.
func NewWorkspace(t *testing.T) Workspace {
	t.Helper()

	rootDir := t.TempDir()
	workspace := Workspace{RootDir: rootDir}
	require.NoError(t, os.MkdirAll(workspace.NodeDataDir(1), 0o755))
	require.NoError(t, os.MkdirAll(workspace.NodeLogDir(1), 0o755))
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
	spec := NodeSpec{
		ID:          1,
		Name:        "node-1",
		RootDir:     s.workspace.NodeRootDir(1),
		DataDir:     s.workspace.NodeDataDir(1),
		ConfigPath:  s.workspace.NodeConfigPath(1),
		StdoutPath:  s.workspace.NodeStdoutPath(1),
		StderrPath:  s.workspace.NodeStderrPath(1),
		ClusterAddr: ports.ClusterAddr,
		GatewayAddr: ports.GatewayAddr,
		APIAddr:     ports.APIAddr,
		ManagerAddr: ports.ManagerAddr,
		LogDir:      s.workspace.NodeLogDir(1),
	}

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

// GatewayAddr returns the public WKProto listen address for the started node.
func (n *StartedNode) GatewayAddr() string {
	return n.Spec.GatewayAddr
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
