package service

import (
	"context"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/service/testkit"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	"github.com/stretchr/testify/require"
)

func TestMemorySequencerAllocatesMonotonicMessageIDs(t *testing.T) {
	seq := &memorySequencer{}

	require.Equal(t, int64(1), seq.NextMessageID())
	require.Equal(t, int64(2), seq.NextMessageID())
	require.Equal(t, int64(3), seq.NextMessageID())
}

func TestMemorySequencerAllocatesPerUserSequences(t *testing.T) {
	seq := &memorySequencer{}

	require.Equal(t, uint32(1), seq.NextUserSequence("u1"))
	require.Equal(t, uint32(2), seq.NextUserSequence("u1"))
	require.Equal(t, uint32(1), seq.NextUserSequence("u2"))
	require.Equal(t, uint32(3), seq.NextUserSequence("u1"))
}

func TestLocalDeliveryWritesFrameToEveryRecipientSession(t *testing.T) {
	s1 := testkit.NewRecordingSession(11, "tcp")
	s2 := testkit.NewRecordingSession(12, "tcp")
	delivery := localDelivery{}

	frame := &wkpacket.PingPacket{}
	err := delivery.Deliver(context.Background(), []SessionMeta{
		{UID: "u2", Session: s1},
		{UID: "u2", Session: s2},
	}, frame)
	require.NoError(t, err)
	require.Len(t, s1.WrittenFrames(), 1)
	require.Len(t, s2.WrittenFrames(), 1)
	require.Same(t, frame, s1.WrittenFrames()[0])
	require.Same(t, frame, s2.WrittenFrames()[0])
}
