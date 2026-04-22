//go:build e2e

package suite

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// Workspace owns the per-test filesystem layout for e2e child processes.
type Workspace struct {
	RootDir string
}

// NewWorkspace creates a temp workspace with a default node-1 tree for phase 1.
func NewWorkspace(t *testing.T) Workspace {
	t.Helper()

	rootDir := t.TempDir()
	workspace := Workspace{RootDir: rootDir}
	require.NoError(t, os.MkdirAll(workspace.NodeDataDir(1), 0o755))
	return workspace
}

// NodeRootDir returns the root directory for one node.
func (w Workspace) NodeRootDir(nodeID uint64) string {
	return filepath.Join(w.RootDir, nodeDirName(nodeID))
}

// NodeDataDir returns the data directory for one node.
func (w Workspace) NodeDataDir(nodeID uint64) string {
	return filepath.Join(w.NodeRootDir(nodeID), "data")
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
