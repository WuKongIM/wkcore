package replica

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCloseStopsCollectorGoroutine(t *testing.T) {
	r := newLeaderReplica(t)
	require.NoError(t, r.Close())

	select {
	case <-r.collectorDone:
	case <-time.After(time.Second):
		t.Fatal("collector goroutine did not stop")
	}

	require.NoError(t, r.Close())
}
