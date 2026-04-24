package store

import (
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/stretchr/testify/require"
)

func TestMessageRowRoundTripPreservesStructuredFields(t *testing.T) {
	row := messageRow{
		MessageSeq:  9,
		MessageID:   42,
		ClientMsgNo: "c-1",
		FromUID:     "u1",
		ChannelID:   "room",
		ChannelType: 1,
		Payload:     []byte("hello"),
		PayloadHash: 123,
	}

	primary, payload, err := encodeMessageFamilies(row)
	require.NoError(t, err)

	decoded, err := decodeMessageFamilies(9, primary, payload)
	require.NoError(t, err)
	require.Equal(t, row.MessageID, decoded.MessageID)
	require.Equal(t, row.Payload, decoded.Payload)
	require.Equal(t, row.PayloadHash, decoded.PayloadHash)
}

func TestMessageRowRejectsZeroMessageID(t *testing.T) {
	_, _, err := encodeMessageFamilies(messageRow{MessageSeq: 1})
	require.Error(t, err)
	require.True(t, errors.Is(err, channel.ErrInvalidArgument) || errors.Is(err, channel.ErrCorruptValue))
}
