package log

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	"github.com/stretchr/testify/require"
)

func TestStoreApplyFetchParsesRealDurableEncodedPayloadForIdempotency(t *testing.T) {
	engine, err := channelstore.Open(t.TempDir())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, engine.Close())
	}()

	key := channel.ChannelKey("channel/1/c1")
	id := channel.ChannelID{ID: "c1", Type: 1}
	st := engine.ForChannel(key, id)

	_, err = st.Append([]channel.Record{{
		Payload: mustEncodeDurableMessage(t, Message{
			MessageID:   11,
			ChannelID:   "c1",
			ChannelType: 1,
			FromUID:     "u1",
			ClientMsgNo: "m1",
			Payload:     []byte("one"),
		}),
		SizeBytes: 1,
	}})
	require.NoError(t, err)

	_, err = st.StoreApplyFetch(channel.ApplyFetchStoreRequest{
		Records: []channel.Record{{
			Payload: mustEncodeDurableMessage(t, Message{
				MessageID:   12,
				ChannelID:   "c1",
				ChannelType: 1,
				FromUID:     "u1",
				ClientMsgNo: "m2",
				Payload:     []byte("two"),
			}),
			SizeBytes: 1,
		}},
		Checkpoint: &channel.Checkpoint{Epoch: 3, HW: 2},
	})
	require.NoError(t, err)

	first, ok, err := st.GetIdempotency(channel.IdempotencyKey{
		ChannelID:   id,
		FromUID:     "u1",
		ClientMsgNo: "m1",
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(11), first.MessageID)
	require.Equal(t, uint64(1), first.MessageSeq)

	second, ok, err := st.GetIdempotency(channel.IdempotencyKey{
		ChannelID:   id,
		FromUID:     "u1",
		ClientMsgNo: "m2",
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(12), second.MessageID)
	require.Equal(t, uint64(2), second.MessageSeq)
}
