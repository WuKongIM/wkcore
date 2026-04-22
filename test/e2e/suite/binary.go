//go:build e2e

package suite

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
)

// BinaryCache builds and caches the e2e wukongim binary once per test process.
type BinaryCache struct {
	once  sync.Once
	path  string
	err   error
	build func(dst string) error
}

// Path returns the cached binary path, building it on first use.
func (c *BinaryCache) Path(tempRoot string) (string, error) {
	c.once.Do(func() {
		c.err = os.MkdirAll(tempRoot, 0o755)
		if c.err != nil {
			return
		}
		c.path = filepath.Join(tempRoot, "wukongim-e2e")
		build := c.build
		if build == nil {
			build = buildBinary
		}
		c.err = build(c.path)
	})
	return c.path, c.err
}

func buildBinary(dst string) error {
	cmd := exec.Command("go", "build", "-o", dst, "./cmd/wukongim")
	cmd.Dir = repoRoot()
	return cmd.Run()
}

func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}
