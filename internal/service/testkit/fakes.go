package testkit

import (
	"sync"

	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

type RecordingSession struct {
	id       uint64
	listener string

	mu            sync.Mutex
	values        map[string]any
	writtenFrames []wkpacket.Frame
}

func NewRecordingSession(id uint64, listener string) *RecordingSession {
	return &RecordingSession{
		id:       id,
		listener: listener,
		values:   make(map[string]any),
	}
}

func (s *RecordingSession) ID() uint64 {
	if s == nil {
		return 0
	}
	return s.id
}

func (s *RecordingSession) Listener() string {
	if s == nil {
		return ""
	}
	return s.listener
}

func (s *RecordingSession) RemoteAddr() string {
	return ""
}

func (s *RecordingSession) LocalAddr() string {
	return ""
}

func (s *RecordingSession) WriteFrame(frame wkpacket.Frame, _ ...session.WriteOption) error {
	if s == nil {
		return session.ErrSessionClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.writtenFrames = append(s.writtenFrames, frame)
	return nil
}

func (s *RecordingSession) Close() error {
	return nil
}

func (s *RecordingSession) SetValue(key string, value any) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.values == nil {
		s.values = make(map[string]any)
	}
	s.values[key] = value
}

func (s *RecordingSession) Value(key string) any {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.values[key]
}

func (s *RecordingSession) WrittenFrames() []wkpacket.Frame {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]wkpacket.Frame, len(s.writtenFrames))
	copy(out, s.writtenFrames)
	return out
}
