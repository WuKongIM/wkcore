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
	st := newTestChannelStore(t)
	require.NoError(t, st.StoreCheckpoint(channel.Checkpoint{Epoch: 2, HW: 8}))
	cp, err := st.LoadCheckpoint()
	require.NoError(t, err)
	require.Equal(t, uint64(8), cp.HW)
}
