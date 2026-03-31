package session

import (
	"errors"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

var (
	ErrSessionClosed    = errors.New("gateway/session: session is closed")
	ErrWriteQueueFull   = errors.New("gateway/session: write queue is full")
	ErrOutboundOverflow = errors.New("gateway/session: outbound bytes limit exceeded")
)

type Session interface {
	ID() uint64
	Listener() string
	RemoteAddr() string
	LocalAddr() string

	WriteFrame(frame wkpacket.Frame, opts ...WriteOption) error
	Close() error

	SetValue(key string, value any)
	Value(key string) any
}

type WriteOption interface {
	apply(*OutboundMeta)
}

type OutboundMeta struct {
	ReplyToken string
}

type replyTokenOption string

func (o replyTokenOption) apply(meta *OutboundMeta) {
	meta.ReplyToken = string(o)
}

func WithReplyToken(token string) WriteOption {
	return replyTokenOption(token)
}

type writeFrameFn func(frame wkpacket.Frame, meta OutboundMeta) error

type session struct {
	id         uint64
	listener   string
	remoteAddr string
	localAddr  string

	values sync.Map

	mu               sync.Mutex
	closed           bool
	writeCh          chan []byte
	outboundBytes    int64
	maxOutboundBytes int64
	writeFrameFn     writeFrameFn
	closeOnce        sync.Once
}

func newSession(id uint64, listener, remoteAddr, localAddr string, writeQueueSize int, maxOutboundBytes int64, writeFrameFn writeFrameFn) *session {
	if writeQueueSize <= 0 {
		writeQueueSize = 1
	}
	if maxOutboundBytes < 0 {
		maxOutboundBytes = 0
	}
	return &session{
		id:               id,
		listener:         listener,
		remoteAddr:       remoteAddr,
		localAddr:        localAddr,
		writeCh:          make(chan []byte, writeQueueSize),
		maxOutboundBytes: maxOutboundBytes,
		writeFrameFn:     writeFrameFn,
	}
}

func (s *session) ID() uint64 {
	if s == nil {
		return 0
	}
	return s.id
}

func (s *session) Listener() string {
	if s == nil {
		return ""
	}
	return s.listener
}

func (s *session) RemoteAddr() string {
	if s == nil {
		return ""
	}
	return s.remoteAddr
}

func (s *session) LocalAddr() string {
	if s == nil {
		return ""
	}
	return s.localAddr
}

func (s *session) WriteFrame(frame wkpacket.Frame, opts ...WriteOption) error {
	if s == nil {
		return ErrSessionClosed
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrSessionClosed
	}
	s.mu.Unlock()

	meta := OutboundMeta{}
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&meta)
		}
	}
	if s.writeFrameFn == nil {
		return nil
	}
	return s.writeFrameFn(frame, meta)
}

func (s *session) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		close(s.writeCh)
		s.mu.Unlock()
	})
	return nil
}

func (s *session) SetValue(key string, value any) {
	if s == nil {
		return
	}
	s.values.Store(key, value)
}

func (s *session) Value(key string) any {
	if s == nil {
		return nil
	}
	value, _ := s.values.Load(key)
	return value
}

func (s *session) enqueueEncoded(payload []byte) error {
	if s == nil {
		return ErrSessionClosed
	}
	queued := append([]byte(nil), payload...)
	size := int64(len(queued))

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSessionClosed
	}
	if s.maxOutboundBytes > 0 && s.outboundBytes+size > s.maxOutboundBytes {
		return ErrOutboundOverflow
	}

	select {
	case s.writeCh <- queued:
		s.outboundBytes += size
		return nil
	default:
		return ErrWriteQueueFull
	}
}

func (s *session) dequeueEncoded() ([]byte, bool) {
	if s == nil {
		return nil, false
	}

	payload, ok := <-s.writeCh
	if !ok {
		return nil, false
	}

	s.mu.Lock()
	s.outboundBytes -= int64(len(payload))
	s.mu.Unlock()

	return payload, true
}
