package node

import (
	"context"
	"sync"
	"testing"

	gatewaysession "github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/stretchr/testify/require"
)

func TestDeliveryRPCDropsWhenBootIDDoesNotMatch(t *testing.T) {
	reg := online.NewRegistry()
	sess := newRecordingSession(11, "tcp")
	require.NoError(t, reg.Register(online.OnlineConn{
		SessionID:   11,
		UID:         "u1",
		GroupID:     1,
		State:       online.LocalRouteStateActive,
		Listener:    "tcp",
		DeviceFlag:  wkframe.APP,
		DeviceLevel: wkframe.DeviceLevelMaster,
		Session:     sess,
	}))

	adapter := New(Options{
		Presence:      presence.New(presence.Options{}),
		GatewayBootID: 99,
		Online:        reg,
	})

	body, err := adapter.handleDeliveryRPC(context.Background(), mustMarshal(t, deliveryRequest{
		UID:        "u1",
		GroupID:    1,
		BootID:     100,
		SessionIDs: []uint64{11},
		Frame:      mustEncodeFrame(t, &wkframe.RecvPacket{MessageID: 1, MessageSeq: 1}),
	}))
	require.NoError(t, err)
	require.Equal(t, "ok", mustDecodeDeliveryResponse(t, body).Status)
	require.Empty(t, sess.WrittenFrames())
}

func TestDeliveryRPCDropsClosingRouteTargets(t *testing.T) {
	reg := online.NewRegistry()
	active := newRecordingSession(11, "tcp")
	closing := newRecordingSession(12, "tcp")
	require.NoError(t, reg.Register(online.OnlineConn{
		SessionID:   11,
		UID:         "u1",
		GroupID:     1,
		State:       online.LocalRouteStateActive,
		Listener:    "tcp",
		DeviceFlag:  wkframe.APP,
		DeviceLevel: wkframe.DeviceLevelMaster,
		Session:     active,
	}))
	require.NoError(t, reg.Register(online.OnlineConn{
		SessionID:   12,
		UID:         "u1",
		GroupID:     1,
		State:       online.LocalRouteStateActive,
		Listener:    "tcp",
		DeviceFlag:  wkframe.APP,
		DeviceLevel: wkframe.DeviceLevelMaster,
		Session:     closing,
	}))
	_, ok := reg.MarkClosing(12)
	require.True(t, ok)

	adapter := New(Options{
		Presence:      presence.New(presence.Options{}),
		GatewayBootID: 99,
		Online:        reg,
	})

	body, err := adapter.handleDeliveryRPC(context.Background(), mustMarshal(t, deliveryRequest{
		UID:        "u1",
		GroupID:    1,
		BootID:     99,
		SessionIDs: []uint64{11, 12},
		Frame:      mustEncodeFrame(t, &wkframe.RecvPacket{MessageID: 1, MessageSeq: 1}),
	}))
	require.NoError(t, err)
	require.Equal(t, "ok", mustDecodeDeliveryResponse(t, body).Status)
	require.Len(t, active.WrittenFrames(), 1)
	require.Empty(t, closing.WrittenFrames())
}

type recordingSession struct {
	id       uint64
	listener string

	mu     sync.Mutex
	frames []wkframe.Frame
	closed bool
}

func newRecordingSession(id uint64, listener string) *recordingSession {
	return &recordingSession{id: id, listener: listener}
}

func (s *recordingSession) ID() uint64 { return s.id }

func (s *recordingSession) Listener() string { return s.listener }

func (s *recordingSession) RemoteAddr() string { return "" }

func (s *recordingSession) LocalAddr() string { return "" }

func (s *recordingSession) WriteFrame(frame wkframe.Frame, _ ...gatewaysession.WriteOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frames = append(s.frames, frame)
	return nil
}

func (s *recordingSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *recordingSession) SetValue(string, any) {}

func (s *recordingSession) Value(string) any { return nil }

func (s *recordingSession) WrittenFrames() []wkframe.Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]wkframe.Frame, len(s.frames))
	copy(out, s.frames)
	return out
}

var _ gatewaysession.Session = (*recordingSession)(nil)
