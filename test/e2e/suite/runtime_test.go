//go:build e2e

package suite

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewWorkspaceCreatesNodeScopedPaths(t *testing.T) {
	workspace := NewWorkspace(t)

	require.DirExists(t, workspace.RootDir)
	require.Equal(t, filepath.Join(workspace.RootDir, "node-1"), workspace.NodeRootDir(1))
	require.Equal(t, filepath.Join(workspace.RootDir, "node-1", "data"), workspace.NodeDataDir(1))
	require.Equal(t, filepath.Join(workspace.RootDir, "node-1", "wukongim.conf"), workspace.NodeConfigPath(1))
	require.Equal(t, filepath.Join(workspace.RootDir, "node-1", "stdout.log"), workspace.NodeStdoutPath(1))
	require.Equal(t, filepath.Join(workspace.RootDir, "node-1", "stderr.log"), workspace.NodeStderrPath(1))
}

func TestNewWorkspaceCreatesNodeScopedLogDirPaths(t *testing.T) {
	workspace := NewWorkspace(t)

	require.Equal(t, filepath.Join(workspace.RootDir, "node-1", "logs"), workspace.NodeLogDir(1))
}
