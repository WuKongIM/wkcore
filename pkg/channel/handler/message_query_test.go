package handler

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	channelstore "github.com/WuKongIM/WuKongIM/pkg/channel/store"
	"github.com/stretchr/testify/require"
)

func TestQueryMessagesReturnsLatestPageDescending(t *testing.T) {
	store := openMessageQueryStore(t)
	id := channel.ChannelID{ID: "room-1", Type: 2}
	appendQueryMessages(t, store, id,
		channel.Message{MessageID: 11, ClientMsgNo: "c1", ChannelID: id.ID, ChannelType: id.Type, FromUID: "u1", Payload: []byte("one")},
		channel.Message{MessageID: 12, ClientMsgNo: "c2", ChannelID: id.ID, ChannelType: id.Type, FromUID: "u1", Payload: []byte("two")},
		channel.Message{MessageID: 13, ClientMsgNo: "c3", ChannelID: id.ID, ChannelType: id.Type, FromUID: "u2", Payload: []byte("three")},
	)

	result, err := QueryMessages(store, 3, QueryMessagesRequest{
		ChannelID: id,
		Limit:     2,
	})
	require.NoError(t, err)
	require.True(t, result.HasMore)
	require.Equal(t, uint64(2), result.NextBeforeSeq)
	require.Len(t, result.Messages, 2)
	require.Equal(t, []uint64{3, 2}, []uint64{result.Messages[0].MessageSeq, result.Messages[1].MessageSeq})
	require.Equal(t, []uint64{13, 12}, []uint64{result.Messages[0].MessageID, result.Messages[1].MessageID})

	nextPage, err := QueryMessages(store, 3, QueryMessagesRequest{
		ChannelID: id,
		Limit:     2,
		BeforeSeq: result.NextBeforeSeq,
	})
	require.NoError(t, err)
	require.False(t, nextPage.HasMore)
	require.Zero(t, nextPage.NextBeforeSeq)
	require.Len(t, nextPage.Messages, 1)
	require.Equal(t, uint64(1), nextPage.Messages[0].MessageSeq)
	require.Equal(t, uint64(11), nextPage.Messages[0].MessageID)
}

func TestQueryMessagesFiltersByClientMsgNoAcrossPages(t *testing.T) {
	store := openMessageQueryStore(t)
	id := channel.ChannelID{ID: "room-2", Type: 2}
	appendQueryMessages(t, store, id,
		channel.Message{MessageID: 21, ClientMsgNo: "same", ChannelID: id.ID, ChannelType: id.Type, FromUID: "u1", Payload: []byte("one")},
		channel.Message{MessageID: 22, ClientMsgNo: "other", ChannelID: id.ID, ChannelType: id.Type, FromUID: "u1", Payload: []byte("two")},
		channel.Message{MessageID: 23, ClientMsgNo: "same", ChannelID: id.ID, ChannelType: id.Type, FromUID: "u2", Payload: []byte("three")},
		channel.Message{MessageID: 24, ClientMsgNo: "other", ChannelID: id.ID, ChannelType: id.Type, FromUID: "u2", Payload: []byte("four")},
		channel.Message{MessageID: 25, ClientMsgNo: "same", ChannelID: id.ID, ChannelType: id.Type, FromUID: "u3", Payload: []byte("five")},
	)

	result, err := QueryMessages(store, 5, QueryMessagesRequest{
		ChannelID:   id,
		Limit:       2,
		ClientMsgNo: "same",
	})
	require.NoError(t, err)
	require.True(t, result.HasMore)
	require.Equal(t, uint64(3), result.NextBeforeSeq)
	require.Equal(t, []uint64{5, 3}, []uint64{result.Messages[0].MessageSeq, result.Messages[1].MessageSeq})

	nextPage, err := QueryMessages(store, 5, QueryMessagesRequest{
		ChannelID:   id,
		Limit:       2,
		ClientMsgNo: "same",
		BeforeSeq:   result.NextBeforeSeq,
	})
	require.NoError(t, err)
	require.False(t, nextPage.HasMore)
	require.Len(t, nextPage.Messages, 1)
	require.Equal(t, uint64(1), nextPage.Messages[0].MessageSeq)
	require.Equal(t, uint64(21), nextPage.Messages[0].MessageID)
}

func TestQueryMessagesFiltersByMessageID(t *testing.T) {
	store := openMessageQueryStore(t)
	id := channel.ChannelID{ID: "room-3", Type: 2}
	appendQueryMessages(t, store, id,
		channel.Message{MessageID: 31, ClientMsgNo: "c1", ChannelID: id.ID, ChannelType: id.Type, FromUID: "u1", Payload: []byte("one")},
		channel.Message{MessageID: 32, ClientMsgNo: "c2", ChannelID: id.ID, ChannelType: id.Type, FromUID: "u1", Payload: []byte("two")},
		channel.Message{MessageID: 33, ClientMsgNo: "c3", ChannelID: id.ID, ChannelType: id.Type, FromUID: "u1", Payload: []byte("three")},
	)

	result, err := QueryMessages(store, 3, QueryMessagesRequest{
		ChannelID: id,
		Limit:     5,
		MessageID: 32,
	})
	require.NoError(t, err)
	require.False(t, result.HasMore)
	require.Zero(t, result.NextBeforeSeq)
	require.Len(t, result.Messages, 1)
	require.Equal(t, uint64(2), result.Messages[0].MessageSeq)
	require.Equal(t, uint64(32), result.Messages[0].MessageID)
}

func openMessageQueryStore(t *testing.T) *channelstore.Engine {
	t.Helper()
	store, err := channelstore.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store
}

func appendQueryMessages(t *testing.T, engine *channelstore.Engine, id channel.ChannelID, messages ...channel.Message) {
	t.Helper()
	st := engine.ForChannel(KeyFromChannelID(id), id)
	records := make([]channel.Record, 0, len(messages))
	for _, message := range messages {
		encoded, err := encodeMessage(message)
		require.NoError(t, err)
		records = append(records, channel.Record{Payload: encoded, SizeBytes: len(encoded)})
	}
	_, err := st.Append(records)
	require.NoError(t, err)
	require.NoError(t, st.StoreCheckpoint(channel.Checkpoint{HW: uint64(len(messages))}))
}
