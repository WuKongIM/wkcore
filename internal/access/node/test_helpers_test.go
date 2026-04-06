package node

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkcodec"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/stretchr/testify/require"
)

func mustMarshal(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	require.NoError(t, err)
	return body
}

func mustEncodeFrame(t *testing.T, frame wkframe.Frame) []byte {
	t.Helper()
	body, err := wkcodec.New().EncodeFrame(frame, wkframe.LatestVersion)
	require.NoError(t, err)
	return body
}

func mustDecodeDeliveryResponse(t *testing.T, body []byte) deliveryResponse {
	t.Helper()
	resp, err := decodeDeliveryResponse(body)
	require.NoError(t, err)
	return resp
}

func mustDecodeDeliveryPushResponse(t *testing.T, body []byte) deliveryPushResponse {
	t.Helper()
	resp, err := decodeDeliveryPushResponse(body)
	require.NoError(t, err)
	return resp
}

func testOnlineConn(sessionID uint64, uid string, groupID uint64) online.OnlineConn {
	return online.OnlineConn{
		SessionID:   sessionID,
		UID:         uid,
		DeviceID:    "d1",
		DeviceFlag:  wkframe.APP,
		DeviceLevel: wkframe.DeviceLevelMaster,
		GroupID:     groupID,
		State:       online.LocalRouteStateActive,
		Listener:    "tcp",
		ConnectedAt: time.Unix(200, 0),
		Session:     session.New(session.Config{ID: sessionID, Listener: "tcp"}),
	}
}

type recordingSession struct {
	id       uint64
	listener string

	mu     sync.Mutex
	frames []wkframe.Frame
}

func newRecordingSession(id uint64, listener string) *recordingSession {
	return &recordingSession{id: id, listener: listener}
}

func (s *recordingSession) ID() uint64 { return s.id }

func (s *recordingSession) Listener() string { return s.listener }

func (s *recordingSession) RemoteAddr() string { return "" }

func (s *recordingSession) LocalAddr() string { return "" }

func (s *recordingSession) WriteFrame(frame wkframe.Frame, _ ...session.WriteOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frames = append(s.frames, frame)
	return nil
}

func (s *recordingSession) Close() error { return nil }

func (s *recordingSession) SetValue(string, any) {}

func (s *recordingSession) Value(string) any { return nil }

func (s *recordingSession) WrittenFrames() []wkframe.Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]wkframe.Frame, len(s.frames))
	copy(out, s.frames)
	return out
}
