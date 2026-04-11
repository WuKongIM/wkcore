package runtime

import (
	"errors"
	"sync"
	"testing"
	"time"

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

func TestEnsureChannelIsAtomicPerKey(t *testing.T) {
	rt := newTestRuntimeWithOptions(t,
		withGenerationStoreDelay(25*time.Millisecond),
		withReplicaFactoryDelay(25*time.Millisecond),
	)
	meta := testMeta("room-atomic")

	const callers = 8
	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			<-start
			errs <- rt.EnsureChannel(meta)
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	var success int
	var exists int
	for err := range errs {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrChannelExists):
			exists++
		default:
			require.NoError(t, err)
		}
	}

	require.Equal(t, 1, success)
	require.Equal(t, callers-1, exists)

	store, ok := rt.generationStore.(*fakeGenerationStore)
	require.True(t, ok)
	require.Equal(t, uint64(1), store.stored[meta.Key])

	factory, ok := rt.replicaFactory.(*fakeReplicaFactory)
	require.True(t, ok)
	require.Len(t, factory.created, 1)
	require.Equal(t, uint64(1), factory.created[0].Generation)
}
