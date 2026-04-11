package store

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/stretchr/testify/require"
)

func TestChannelStoreAppendsAndReadsRecords(t *testing.T) {
	st := newTestChannelStore(t)
	base, err := st.Append([]channel.Record{{Payload: []byte("a"), SizeBytes: 1}})
	require.NoError(t, err)
	require.Equal(t, uint64(0), base)

	records, err := st.Read(0, 1024)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, []byte("a"), records[0].Payload)
}

func TestChannelStorePersistsCheckpointAndHistory(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	require.NoError(t, err)

	key := channel.ChannelKey("channel/1/history")
	id := channel.ChannelID{ID: "history", Type: 1}
	st := engine.ForChannel(key, id)

	require.NoError(t, st.StoreCheckpoint(channel.Checkpoint{Epoch: 2, HW: 8}))
	require.NoError(t, st.AppendHistory(channel.EpochPoint{Epoch: 2, StartOffset: 6}))
	require.NoError(t, engine.Close())

	reopened, err := Open(dir)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reopened.Close())
	}()

	reloaded := reopened.ForChannel(key, id)

	cp, err := reloaded.LoadCheckpoint()
	require.NoError(t, err)
	require.Equal(t, uint64(8), cp.HW)

	history, err := reloaded.LoadHistory()
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.Equal(t, channel.EpochPoint{Epoch: 2, StartOffset: 6}, history[0])
}
