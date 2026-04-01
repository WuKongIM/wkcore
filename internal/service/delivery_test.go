package service

import (
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
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

func TestMemorySequencerAllocatesPerChannelSequences(t *testing.T) {
	seq := &memorySequencer{}

	require.Equal(t, uint32(1), seq.NextChannelSequence("u1"))
	require.Equal(t, uint32(2), seq.NextChannelSequence("u1"))
	require.Equal(t, uint32(1), seq.NextChannelSequence("u2"))
	require.Equal(t, uint32(3), seq.NextChannelSequence("u1"))
}

func TestLocalDeliveryWritesFrameToEveryRecipientSession(t *testing.T) {
	s1 := testkit.NewRecordingSession(11, "tcp")
	s2 := testkit.NewRecordingSession(12, "tcp")
	delivery := localDelivery{}

	frame := &wkpacket.PingPacket{}
	err := delivery.Deliver([]SessionMeta{
		{UID: "u2", Session: s1},
		{UID: "u2", Session: s2},
	}, frame)
	require.NoError(t, err)
	require.Len(t, s1.WrittenFrames(), 1)
	require.Len(t, s2.WrittenFrames(), 1)
	require.Same(t, frame, s1.WrittenFrames()[0])
	require.Same(t, frame, s2.WrittenFrames()[0])
}

func TestLocalDeliveryContinuesAfterWriteFrameError(t *testing.T) {
	writeErr := errors.New("boom")
	first := &erroringSession{RecordingSession: testkit.NewRecordingSession(11, "tcp"), err: writeErr}
	second := testkit.NewRecordingSession(12, "tcp")
	delivery := localDelivery{}

	err := delivery.Deliver([]SessionMeta{
		{UID: "u2", Session: first},
		{UID: "u2", Session: second},
	}, &wkpacket.PingPacket{})

	require.ErrorIs(t, err, writeErr)
	require.Equal(t, 1, first.writeAttempts)
	require.Len(t, second.WrittenFrames(), 1)
}

type erroringSession struct {
	*testkit.RecordingSession
	err           error
	writeAttempts int
}

func (s *erroringSession) WriteFrame(frame wkpacket.Frame, opts ...session.WriteOption) error {
	s.writeAttempts++
	_ = frame
	_ = opts
	return s.err
}
