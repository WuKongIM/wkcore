package message

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecvAckIsNoop(t *testing.T) {
	app := New(Options{})

	require.NoError(t, app.RecvAck(RecvAckCommand{
		UID:        "u1",
		MessageID:  88,
		MessageSeq: 9,
	}))
}
