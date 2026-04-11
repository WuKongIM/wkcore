package runtime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExpiredTombstonesDoNotBlockChannelLookup(t *testing.T) {
	rt := newTestRuntime(t)
	rt.tombstones.add(testMeta("room-2").Key, 1, time.Now().Add(-time.Second))
	rt.tombstones.dropExpired(time.Now())
	_, ok := rt.Channel(testMeta("room-2").Key)
	require.False(t, ok)
}
