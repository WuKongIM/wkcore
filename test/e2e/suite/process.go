//go:build e2e

package suite

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const defaultStopTimeout = 5 * time.Second

// NodeProcess wraps one real child process used by the e2e suite.
type NodeProcess struct {
	Spec         NodeSpec
	BinaryPath   string
	StartTimeout time.Duration
	StopTimeout  time.Duration

	Cmd       *exec.Cmd
	StdoutLog *os.File
	StderrLog *os.File

	command *exec.Cmd
}

// Start launches the child process and redirects stdout and stderr to files.
func (p *NodeProcess) Start() error {
	rootDir := p.Spec.RootDir
	if rootDir == "" {
		rootDir = filepath.Dir(p.Spec.StdoutPath)
	}
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return err
	}

	stdoutLog, err := os.Create(p.Spec.StdoutPath)
	if err != nil {
		return err
	}
	stderrLog, err := os.Create(p.Spec.StderrPath)
	if err != nil {
		_ = stdoutLog.Close()
		return err
	}

	cmd := p.command
	if cmd == nil {
		cmd = exec.Command(p.BinaryPath, "-config", p.Spec.ConfigPath)
	}
	cmd.Stdout = stdoutLog
	cmd.Stderr = stderrLog

	p.StdoutLog = stdoutLog
	p.StderrLog = stderrLog
	p.Cmd = cmd

	if err := cmd.Start(); err != nil {
		_ = stdoutLog.Close()
		_ = stderrLog.Close()
		return err
	}
	return nil
}

// Stop terminates the child process and waits for it to exit.
func (p *NodeProcess) Stop() error {
	if p == nil || p.Cmd == nil || p.Cmd.Process == nil {
		return nil
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- p.Cmd.Wait()
	}()

	if err := p.Cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return err
	}

	timeout := p.StopTimeout
	if timeout <= 0 {
		timeout = defaultStopTimeout
	}

	select {
	case err := <-waitCh:
		p.closeLogs()
		return normalizeStopError(err)
	case <-time.After(timeout):
		if err := p.Cmd.Process.Kill(); err != nil {
			p.closeLogs()
			return err
		}
		err := <-waitCh
		p.closeLogs()
		return normalizeStopError(err)
	}
}

// DumpDiagnostics returns a small human-readable snapshot of process artifacts.
func (p *NodeProcess) DumpDiagnostics() string {
	var b strings.Builder
	fmt.Fprintf(&b, "config: %s\n", p.Spec.ConfigPath)
	fmt.Fprintf(&b, "stdout: %s\n", p.Spec.StdoutPath)
	fmt.Fprintf(&b, "stderr: %s\n", p.Spec.StderrPath)
	appendLog(&b, "stdout", p.Spec.StdoutPath)
	appendLog(&b, "stderr", p.Spec.StderrPath)
	return b.String()
}

func (p *NodeProcess) closeLogs() {
	if p.StdoutLog != nil {
		_ = p.StdoutLog.Close()
	}
	if p.StderrLog != nil {
		_ = p.StderrLog.Close()
	}
}

func appendLog(b *strings.Builder, name, path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(b, "%s-read-error: %v\n", name, err)
		return
	}
	fmt.Fprintf(b, "%s-content:\n%s", name, data)
	if len(data) > 0 && data[len(data)-1] != '\n' {
		b.WriteByte('\n')
	}
}

func normalizeStopError(err error) error {
	if err == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return nil
	}
	return err
}
