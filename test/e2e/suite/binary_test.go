//go:build e2e

package suite

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBinaryCacheBuildsOncePerProcess(t *testing.T) {
	var calls int
	cache := BinaryCache{
		build: func(dst string) error {
			calls++
			return os.WriteFile(dst, []byte("fake-binary"), 0o755)
		},
	}

	first, err := cache.Path(t.TempDir())
	require.NoError(t, err)
	second, err := cache.Path(t.TempDir())
	require.NoError(t, err)

	require.Equal(t, 1, calls)
	require.Equal(t, first, second)
	require.FileExists(t, first)
}
