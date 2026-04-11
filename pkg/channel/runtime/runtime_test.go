package runtime

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
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

func TestEnsureChannelRespectsMaxChannelsUnderConcurrency(t *testing.T) {
	const maxChannels = 3
	keys := distinctShardKeys(t, 10)
	rt := newTestRuntimeWithOptions(
		t,
		withMaxChannels(maxChannels),
		withGenerationStoreDelay(25*time.Millisecond),
		withReplicaFactoryDelay(25*time.Millisecond),
	)

	start := make(chan struct{})
	errs := make(chan error, len(keys))
	var wg sync.WaitGroup
	wg.Add(len(keys))
	for _, key := range keys {
		meta := testMeta(string(key))
		go func(meta core.Meta) {
			defer wg.Done()
			<-start
			errs <- rt.EnsureChannel(meta)
		}(meta)
	}

	close(start)
	wg.Wait()
	close(errs)

	var success int
	var tooMany int
	for err := range errs {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrTooManyChannels):
			tooMany++
		default:
			require.NoError(t, err)
		}
	}
	require.Equal(t, maxChannels, success)
	require.Equal(t, len(keys)-maxChannels, tooMany)
	require.Equal(t, maxChannels, rt.totalChannels())

	factory, ok := rt.replicaFactory.(*fakeReplicaFactory)
	require.True(t, ok)
	require.Len(t, factory.created, maxChannels)
}

func TestRemoveChannelTombstoneFailureKeepsChannel(t *testing.T) {
	meta := testMeta("room-remove-failure")
	rt := newTestRuntimeWithOptions(t, withTombstoneError(meta.Key, errors.New("boom")))
	require.NoError(t, rt.EnsureChannel(meta))

	err := rt.RemoveChannel(meta.Key)
	require.Error(t, err)

	ch, ok := rt.Channel(meta.Key)
	require.True(t, ok)
	require.Equal(t, meta.Key, ch.ID())
	require.False(t, rt.tombstones.contains(meta.Key, 1))
}

func distinctShardKeys(t *testing.T, n int) []core.ChannelKey {
	t.Helper()

	keys := make([]core.ChannelKey, 0, n)
	seen := make(map[uint32]struct{}, n)
	for i := 0; i < 10000 && len(keys) < n; i++ {
		key := core.ChannelKey(fmt.Sprintf("room-limit-%d", i))
		idx := shardIndex(key)
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		keys = append(keys, key)
	}
	require.Len(t, keys, n)
	return keys
}
