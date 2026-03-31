package gnet

import (
	"sync"

	"github.com/WuKongIM/WuKongIM/internal/gateway/transport"
	gnetv2 "github.com/panjf2000/gnet/v2"
)

type connEventKind uint8

const (
	connEventOpen connEventKind = iota + 1
	connEventData
	connEventClose
)

type connEvent struct {
	kind connEventKind
	data []byte
	err  error
}

type connState struct {
	raw        gnetv2.Conn
	runtime    *listenerRuntime
	transport  *stateConn
	id         uint64
	generation uint64
	localAddr  string
	remoteAddr string

	mu      sync.Mutex
	queue   []connEvent
	wake    chan struct{}
	closing bool
}

func newConnState(id uint64, raw gnetv2.Conn, runtime *listenerRuntime) *connState {
	localAddr := raw.LocalAddr().String()
	if runtime != nil {
		if addr := runtime.addr(); addr != "" {
			localAddr = addr
		}
	}

	state := &connState{
		raw:        raw,
		runtime:    runtime,
		id:         id,
		localAddr:  localAddr,
		remoteAddr: raw.RemoteAddr().String(),
		wake:       make(chan struct{}, 1),
	}
	state.transport = &stateConn{state: state}
	return state
}

func (s *connState) start() {
	go s.run()
}

func (s *connState) enqueueOpen() {
	s.enqueue(connEvent{kind: connEventOpen})
}

func (s *connState) enqueueData(data []byte) {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	s.queue = append(s.queue, connEvent{kind: connEventData, data: data})
	s.mu.Unlock()
	s.signal()
}

func (s *connState) enqueueClose(err error) {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	s.closing = true
	s.queue = append(s.queue, connEvent{kind: connEventClose, err: err})
	s.mu.Unlock()
	s.signal()
}

func (s *connState) fail(err error) {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	s.closing = true
	s.queue = append(s.queue[:0], connEvent{kind: connEventClose, err: err})
	s.mu.Unlock()
	s.signal()
}

func (s *connState) enqueue(event connEvent) {
	s.mu.Lock()
	if s.closing && event.kind != connEventClose {
		s.mu.Unlock()
		return
	}
	if event.kind == connEventClose {
		s.closing = true
	}
	s.queue = append(s.queue, event)
	s.mu.Unlock()
	s.signal()
}

func (s *connState) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *connState) nextEvent() (connEvent, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 {
		return connEvent{}, false
	}
	event := s.queue[0]
	s.queue = s.queue[1:]
	return event, true
}

func (s *connState) run() {
	for {
		event, ok := s.nextEvent()
		if !ok {
			<-s.wake
			continue
		}

		switch event.kind {
		case connEventOpen:
			if s.runtime.handler == nil || !s.runtime.shouldDispatch(s) {
				continue
			}
			if err := s.runtime.handler.OnOpen(s.transport); err != nil {
				s.fail(err)
				_ = s.raw.Close()
			}
		case connEventData:
			if s.runtime.handler == nil || !s.runtime.shouldDispatch(s) {
				continue
			}
			if err := s.runtime.handler.OnData(s.transport, event.data); err != nil {
				s.fail(err)
				_ = s.raw.Close()
			}
		case connEventClose:
			if s.runtime.handler != nil {
				s.runtime.handler.OnClose(s.transport, event.err)
			}
			return
		}
	}
}

type stateConn struct {
	state *connState
}

func (c *stateConn) ID() uint64 {
	return c.state.id
}

func (c *stateConn) Write(data []byte) error {
	payload := append([]byte(nil), data...)
	return c.state.raw.AsyncWrite(payload, nil)
}

func (c *stateConn) Close() error {
	return c.state.raw.Close()
}

func (c *stateConn) LocalAddr() string {
	return c.state.localAddr
}

func (c *stateConn) RemoteAddr() string {
	return c.state.remoteAddr
}

var _ transport.Conn = (*stateConn)(nil)
