package runtime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExpiredTombstonesDoNotBlockChannelLookup(t *testing.T) {
	rt := newTestRuntime(t)
	meta := testMeta("room-2")
	now := time.Now()

	rt.tombstones.add(meta.Key, 1, now.Add(-time.Second))
	rt.tombstones.add(meta.Key, 2, now.Add(time.Second))
	require.True(t, rt.tombstones.contains(meta.Key, 1))
	require.True(t, rt.tombstones.contains(meta.Key, 2))

	rt.tombstones.dropExpired(now)

	require.False(t, rt.tombstones.contains(meta.Key, 1))
	require.True(t, rt.tombstones.contains(meta.Key, 2))
	_, ok := rt.Channel(meta.Key)
	require.False(t, ok)
}
