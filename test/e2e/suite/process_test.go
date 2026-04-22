//go:build e2e

package suite

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNodeProcessStopPrefersSIGTERMBeforeKill(t *testing.T) {
	if os.Getenv("WK_E2E_HELPER_PROCESS") == "trap-sigterm" {
		runTrapSIGTERMHelper()
		return
	}

	workdir := t.TempDir()
	stdoutPath := filepath.Join(workdir, "stdout.log")
	stderrPath := filepath.Join(workdir, "stderr.log")

	cmd := exec.Command(os.Args[0], "-test.run=TestNodeProcessStopPrefersSIGTERMBeforeKill")
	cmd.Env = append(os.Environ(), "WK_E2E_HELPER_PROCESS=trap-sigterm")

	process := NodeProcess{
		Spec: NodeSpec{
			ConfigPath: stdoutPath + ".conf",
			StdoutPath: stdoutPath,
			StderrPath: stderrPath,
		},
		StartTimeout: 2 * time.Second,
		StopTimeout:  2 * time.Second,
		command:      cmd,
	}

	require.NoError(t, process.Start())
	require.Eventually(t, func() bool {
		stdout, err := os.ReadFile(stdoutPath)
		return err == nil && strings.Contains(string(stdout), "helper-started")
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, process.Stop())

	stdout, err := os.ReadFile(stdoutPath)
	require.NoError(t, err)
	require.Contains(t, string(stdout), "got-sigterm")
}

func TestNodeProcessDumpDiagnosticsIncludesConfigAndLogPaths(t *testing.T) {
	workdir := t.TempDir()
	configPath := filepath.Join(workdir, "wukongim.conf")
	stdoutPath := filepath.Join(workdir, "stdout.log")
	stderrPath := filepath.Join(workdir, "stderr.log")

	require.NoError(t, os.WriteFile(stdoutPath, []byte("stdout-line\n"), 0o644))
	require.NoError(t, os.WriteFile(stderrPath, []byte("stderr-line\n"), 0o644))

	process := NodeProcess{
		Spec: NodeSpec{
			ConfigPath: configPath,
			StdoutPath: stdoutPath,
			StderrPath: stderrPath,
		},
	}

	diagnostics := process.DumpDiagnostics()
	require.Contains(t, diagnostics, configPath)
	require.Contains(t, diagnostics, stdoutPath)
	require.Contains(t, diagnostics, stderrPath)
	require.Contains(t, diagnostics, "stdout-line")
	require.Contains(t, diagnostics, "stderr-line")
}

func runTrapSIGTERMHelper() {
	fmt.Println("helper-started")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	<-sigCh
	fmt.Println("got-sigterm")
	os.Exit(0)
}
