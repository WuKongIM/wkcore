package channel

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetaCarriesRuntimeAndBusinessFields(t *testing.T) {
	meta := Meta{
		Key:      ChannelKey("channel/1/dTE="),
		ID:       ChannelID{ID: "u1", Type: 1},
		Leader:   100,
		Status:   StatusActive,
		Features: Features{MessageSeqFormat: MessageSeqFormatU64},
	}
	require.Equal(t, ChannelID{ID: "u1", Type: 1}, meta.ID)
	require.Equal(t, StatusActive, meta.Status)
}

func TestAppendRequestCarriesBusinessChannelID(t *testing.T) {
	req := AppendRequest{
		ChannelID: ChannelID{ID: "room-1", Type: 2},
		Message:   Message{Payload: []byte("hi")},
	}
	require.Equal(t, "room-1", req.ChannelID.ID)
	require.Equal(t, uint8(2), req.ChannelID.Type)
}
