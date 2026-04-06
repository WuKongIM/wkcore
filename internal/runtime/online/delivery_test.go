package online

import (
	"errors"
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/stretchr/testify/require"
)

func TestLocalDeliveryWritesFrameToEveryRecipientSession(t *testing.T) {
	s1 := newRecordingSession(11, "tcp")
	s2 := newRecordingSession(12, "tcp")
	delivery := LocalDelivery{}

	frame := &wkframe.PingPacket{}
	err := delivery.Deliver([]OnlineConn{
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
	first := &erroringSession{recordingSession: newRecordingSession(11, "tcp"), err: writeErr}
	second := newRecordingSession(12, "tcp")
	delivery := LocalDelivery{}

	err := delivery.Deliver([]OnlineConn{
		{UID: "u2", Session: first},
		{UID: "u2", Session: second},
	}, &wkframe.PingPacket{})

	require.ErrorIs(t, err, writeErr)
	require.Equal(t, 1, first.writeAttempts)
	require.Len(t, second.WrittenFrames(), 1)
}

func TestLocalDeliverySkipsClosingRoutes(t *testing.T) {
	active := newRecordingSession(11, "tcp")
	closing := newRecordingSession(12, "tcp")
	delivery := LocalDelivery{}

	err := delivery.Deliver([]OnlineConn{
		{UID: "u2", Session: active, State: LocalRouteStateActive},
		{UID: "u2", Session: closing, State: LocalRouteStateClosing},
	}, &wkframe.PingPacket{})

	require.NoError(t, err)
	require.Len(t, active.WrittenFrames(), 1)
	require.Empty(t, closing.WrittenFrames())
}

type erroringSession struct {
	*recordingSession
	err           error
	writeAttempts int
}

func (s *erroringSession) WriteFrame(frame wkframe.Frame, opts ...session.WriteOption) error {
	s.writeAttempts++
	_ = frame
	_ = opts
	return s.err
}

type recordingSession struct {
	id       uint64
	listener string

	mu            sync.Mutex
	writtenFrames []wkframe.Frame
}

func newRecordingSession(id uint64, listener string) *recordingSession {
	return &recordingSession{id: id, listener: listener}
}

func (s *recordingSession) ID() uint64 {
	return s.id
}

func (s *recordingSession) Listener() string {
	return s.listener
}

func (s *recordingSession) RemoteAddr() string {
	return ""
}

func (s *recordingSession) LocalAddr() string {
	return ""
}

func (s *recordingSession) WriteFrame(frame wkframe.Frame, _ ...session.WriteOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.writtenFrames = append(s.writtenFrames, frame)
	return nil
}

func (s *recordingSession) Close() error {
	return nil
}

func (s *recordingSession) SetValue(string, any) {}

func (s *recordingSession) Value(string) any {
	return nil
}

func (s *recordingSession) WrittenFrames() []wkframe.Frame {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]wkframe.Frame, len(s.writtenFrames))
	copy(out, s.writtenFrames)
	return out
}
