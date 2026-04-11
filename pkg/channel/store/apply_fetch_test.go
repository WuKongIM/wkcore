package store

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/stretchr/testify/require"
)

func TestChannelStoreApplyFetchPersistsCommittedIdempotencyForExistingAndNewRecords(t *testing.T) {
	id := channel.ChannelID{ID: "c1", Type: 1}
	st := openTestChannelStore(t, channel.ChannelKey("channel/1/c1"), id)

	_, err := st.Append([]channel.Record{{
		Payload:   mustEncodeApplyFetchMessagePayload(t, 11, "u1", "m1", "one"),
		SizeBytes: 1,
	}})
	require.NoError(t, err)

	_, err = st.StoreApplyFetch(channel.ApplyFetchStoreRequest{
		Records: []channel.Record{{
			Payload:   mustEncodeApplyFetchMessagePayload(t, 12, "u1", "m2", "two"),
			SizeBytes: 1,
		}},
		Checkpoint: &channel.Checkpoint{Epoch: 3, HW: 2},
	})
	require.NoError(t, err)

	legacy, ok, err := st.GetIdempotency(channel.IdempotencyKey{
		ChannelID:   id,
		FromUID:     "u1",
		ClientMsgNo: "m1",
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(11), legacy.MessageID)
	require.Equal(t, uint64(1), legacy.MessageSeq)
	require.Equal(t, uint64(0), legacy.Offset)

	current, ok, err := st.GetIdempotency(channel.IdempotencyKey{
		ChannelID:   id,
		FromUID:     "u1",
		ClientMsgNo: "m2",
	})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(12), current.MessageID)
	require.Equal(t, uint64(2), current.MessageSeq)
	require.Equal(t, uint64(1), current.Offset)
}

func mustEncodeApplyFetchMessagePayload(t *testing.T, messageID uint64, fromUID, clientMsgNo, body string) []byte {
	t.Helper()

	var payload bytes.Buffer
	require.NoError(t, payload.WriteByte(1))
	require.NoError(t, binary.Write(&payload, binary.BigEndian, messageID))
	payload.Write(make([]byte, 36))                   // codec header fields not used by StoreApplyFetch parsing.
	writeSizedBytes(t, &payload, nil)                 // msgKey
	writeSizedBytes(t, &payload, []byte(clientMsgNo)) // clientMsgNo
	writeSizedBytes(t, &payload, nil)                 // streamNo
	writeSizedBytes(t, &payload, nil)                 // channelID
	writeSizedBytes(t, &payload, nil)                 // topic
	writeSizedBytes(t, &payload, []byte(fromUID))     // fromUID
	writeSizedBytes(t, &payload, []byte(body))        // payload
	return payload.Bytes()
}

func writeSizedBytes(t *testing.T, buf *bytes.Buffer, value []byte) {
	t.Helper()
	require.NoError(t, binary.Write(buf, binary.BigEndian, uint32(len(value))))
	_, err := buf.Write(value)
	require.NoError(t, err)
}
