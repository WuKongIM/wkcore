package runtime

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChannelLookupUsesReadOnlyPath(t *testing.T) {
	rt := newTestRuntime(t)
	meta := testMeta("room-1")
	require.NoError(t, rt.EnsureChannel(meta))

	ch, ok := rt.Channel(meta.Key)
	require.True(t, ok)
	require.Equal(t, meta.Key, ch.ID())
}

func TestEnsureChannel(t *testing.T) {
	rt := newTestRuntime(t)
	meta := testMeta("room-ensure")

	require.NoError(t, rt.EnsureChannel(meta))

	_, ok := rt.Channel(meta.Key)
	require.True(t, ok)
	store, ok := rt.generationStore.(*fakeGenerationStore)
	require.True(t, ok)
	require.Equal(t, uint64(1), store.stored[meta.Key])
}

func TestRemoveChannel(t *testing.T) {
	rt := newTestRuntime(t)
	meta := testMeta("room-remove")
	require.NoError(t, rt.EnsureChannel(meta))

	require.NoError(t, rt.RemoveChannel(meta.Key))

	_, ok := rt.Channel(meta.Key)
	require.False(t, ok)
}
