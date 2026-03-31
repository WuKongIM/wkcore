package core

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway/protocol"
	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/internal/gateway/transport"
	gatewaytypes "github.com/WuKongIM/WuKongIM/internal/gateway/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

var (
	ErrNilRegistry       = errors.New("gateway/core: nil registry")
	ErrNilOptions        = errors.New("gateway/core: nil options")
	ErrDecodeNoProgress  = errors.New("gateway/core: decode returned frames without consuming bytes")
	ErrInvalidDecodeStep = errors.New("gateway/core: decode consumed invalid byte count")
)

type Server struct {
	registry   *Registry
	options    gatewaytypes.Options
	dispatcher dispatcher
	sessions   *session.Manager

	nextSessionID atomic.Uint64

	mu        sync.Mutex
	started   bool
	stopped   bool
	listeners []*listenerRuntime
	states    map[connKey]*sessionState

	workerWG sync.WaitGroup
}

type listenerRuntime struct {
	options  gatewaytypes.ListenerOptions
	factory  transport.Factory
	adapter  protocol.Adapter
	tracker  protocol.ReplyTokenTracker
	listener transport.Listener
}

type connKey struct {
	listener string
	connID   uint64
}

type sessionState struct {
	server   *Server
	listener *listenerRuntime
	conn     transport.Conn
	session  session.Session
	queue    session.EncodedQueue
	key      connKey

	inboundMu sync.Mutex
	inbound   []byte

	metaMu           sync.RWMutex
	closeReasonValue gatewaytypes.CloseReason

	closeOnce sync.Once
	closedCh  chan struct{}

	lastActivity atomic.Int64
}

func NewServer(registry *Registry, opts *gatewaytypes.Options) (*Server, error) {
	if registry == nil {
		return nil, ErrNilRegistry
	}
	if opts == nil {
		return nil, ErrNilOptions
	}

	cfg := gatewaytypes.Options{
		Handler:        opts.Handler,
		DefaultSession: opts.DefaultSession,
		Listeners:      append([]gatewaytypes.ListenerOptions(nil), opts.Listeners...),
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	listeners := make([]*listenerRuntime, 0, len(cfg.Listeners))
	for _, listener := range cfg.Listeners {
		factory, err := registry.Transport(listener.Transport)
		if err != nil {
			return nil, err
		}
		adapter, err := registry.Protocol(listener.Protocol)
		if err != nil {
			return nil, err
		}

		runtime := &listenerRuntime{
			options: listener,
			factory: factory,
			adapter: adapter,
		}
		if tracker, ok := adapter.(protocol.ReplyTokenTracker); ok {
			runtime.tracker = tracker
		}
		listeners = append(listeners, runtime)
	}

	return &Server{
		registry:   registry,
		options:    cfg,
		dispatcher: newDispatcher(cfg.Handler),
		sessions:   session.NewManager(),
		listeners:  listeners,
		states:     make(map[connKey]*sessionState),
	}, nil
}

func (s *Server) Start() error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return gatewaytypes.ErrGatewayClosed
	}
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.started = true
	runtimes := append([]*listenerRuntime(nil), s.listeners...)
	s.mu.Unlock()

	started := make([]transport.Listener, 0, len(runtimes))
	for _, runtime := range runtimes {
		runtime := runtime
		listener, err := runtime.factory.New(transport.ListenerOptions{
			Name:    runtime.options.Name,
			Network: runtime.options.Network,
			Address: runtime.options.Address,
			Path:    runtime.options.Path,
			OnError: func(err error) {
				s.dispatcher.listenerError(runtime.options.Name, err)
			},
		}, &connHandler{server: s, listener: runtime})
		if err != nil {
			s.rollbackStart(started)
			s.mu.Lock()
			s.started = false
			s.mu.Unlock()
			return err
		}

		runtime.listener = listener
		if err := listener.Start(); err != nil {
			s.dispatcher.listenerError(runtime.options.Name, err)
			_ = listener.Stop()
			s.rollbackStart(started)
			s.mu.Lock()
			s.started = false
			s.mu.Unlock()
			return err
		}
		started = append(started, listener)
	}

	return nil
}

func (s *Server) Stop() error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true

	listeners := make([]transport.Listener, 0, len(s.listeners))
	for _, runtime := range s.listeners {
		if runtime.listener != nil {
			listeners = append(listeners, runtime.listener)
		}
	}

	states := make([]*sessionState, 0, len(s.states))
	for _, state := range s.states {
		states = append(states, state)
	}
	s.mu.Unlock()

	var firstErr error
	for _, listener := range listeners {
		if err := listener.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for _, state := range states {
		state.close(gatewaytypes.CloseReasonServerStop, nil)
	}

	s.workerWG.Wait()
	return firstErr
}

type connHandler struct {
	server   *Server
	listener *listenerRuntime
}

func (h *connHandler) OnOpen(conn transport.Conn) error {
	if h == nil || h.server == nil {
		return nil
	}
	return h.server.onOpen(h.listener, conn)
}

func (h *connHandler) OnData(conn transport.Conn, data []byte) error {
	if h == nil || h.server == nil {
		return nil
	}
	return h.server.onData(h.listener, conn, data)
}

func (h *connHandler) OnClose(conn transport.Conn, err error) {
	if h == nil || h.server == nil {
		return
	}
	h.server.onClose(h.listener, conn, err)
}

func (s *Server) onOpen(listener *listenerRuntime, conn transport.Conn) error {
	if listener == nil || conn == nil {
		return nil
	}

	state := &sessionState{
		server:   s,
		listener: listener,
		conn:     conn,
		key: connKey{
			listener: listener.options.Name,
			connID:   conn.ID(),
		},
		closedCh: make(chan struct{}),
	}
	state.touchActivity()

	var sess session.Session
	sess = session.New(session.Config{
		ID:               s.nextSessionID.Add(1),
		Listener:         listener.options.Name,
		RemoteAddr:       conn.RemoteAddr(),
		LocalAddr:        conn.LocalAddr(),
		WriteQueueSize:   s.options.DefaultSession.WriteQueueSize,
		MaxOutboundBytes: int64(s.options.DefaultSession.MaxOutboundBytes),
		WriteFrameFn: func(frame wkpacket.Frame, meta session.OutboundMeta) error {
			return s.encodeAndQueue(state, frame, meta)
		},
	})

	queue, ok := sess.(session.EncodedQueue)
	if !ok {
		return fmt.Errorf("gateway/core: session %T does not implement encoded queue", sess)
	}

	state.session = sess
	state.queue = queue
	s.registerState(state)

	if err := listener.adapter.OnOpen(sess); err != nil {
		state.close(gatewaytypes.CloseReasonProtocolError, err)
		return nil
	}

	if err := s.dispatcher.sessionOpen(state); err != nil {
		s.handleHandlerError(state, err)
	}
	if state.isClosed() {
		return nil
	}

	s.startWriter(state)
	s.startIdleMonitor(state)
	return nil
}

func (s *Server) onData(listener *listenerRuntime, conn transport.Conn, data []byte) error {
	if listener == nil || conn == nil {
		return nil
	}

	state := s.state(listener.options.Name, conn.ID())
	if state == nil || state.isClosed() {
		return nil
	}

	state.inboundMu.Lock()
	defer state.inboundMu.Unlock()

	if state.isClosed() {
		return nil
	}

	state.touchActivity()
	state.inbound = append(state.inbound, data...)
	if limit := s.options.DefaultSession.MaxInboundBytes; limit > 0 && len(state.inbound) > limit {
		state.close(gatewaytypes.CloseReasonInboundOverflow, gatewaytypes.ErrInboundOverflow)
		return nil
	}

	for !state.isClosed() {
		frames, consumed, err := listener.adapter.Decode(state.session, state.inbound)
		if err != nil {
			state.close(gatewaytypes.CloseReasonProtocolError, err)
			return nil
		}
		if consumed < 0 || consumed > len(state.inbound) {
			state.close(gatewaytypes.CloseReasonProtocolError, ErrInvalidDecodeStep)
			return nil
		}
		if consumed == 0 && len(frames) == 0 {
			return nil
		}
		if consumed == 0 {
			state.close(gatewaytypes.CloseReasonProtocolError, ErrDecodeNoProgress)
			return nil
		}

		state.inbound = state.inbound[consumed:]
		tokens := s.replyTokens(listener, state.session, len(frames))
		for i, frame := range frames {
			replyToken := ""
			if i < len(tokens) {
				replyToken = tokens[i]
			}
			if err := s.dispatcher.frame(state, replyToken, frame); err != nil {
				s.handleHandlerError(state, err)
				if state.isClosed() {
					return nil
				}
			}
		}
	}

	return nil
}

func (s *Server) onClose(listener *listenerRuntime, conn transport.Conn, err error) {
	if listener == nil || conn == nil {
		return
	}

	state := s.state(listener.options.Name, conn.ID())
	if state == nil {
		return
	}

	if err != nil {
		state.close(gatewaytypes.CloseReasonPeerClosed, err)
		return
	}
	state.close(gatewaytypes.CloseReasonPeerClosed, nil)
}

func (s *Server) encodeAndQueue(state *sessionState, frame wkpacket.Frame, meta session.OutboundMeta) error {
	if state == nil || state.listener == nil || state.queue == nil {
		return session.ErrSessionClosed
	}

	encoded, err := state.listener.adapter.Encode(state.session, frame, meta)
	if err != nil {
		return err
	}
	return state.queue.EnqueueEncoded(encoded)
}

func (s *Server) startWriter(state *sessionState) {
	if state == nil || state.queue == nil || state.conn == nil {
		return
	}

	s.workerWG.Add(1)
	go func() {
		defer s.workerWG.Done()
		for {
			payload, ok := state.queue.DequeueEncoded()
			if !ok {
				return
			}
			if err := s.writePayload(state, payload); err != nil {
				reason := gatewaytypes.CloseReasonPeerClosed
				reportErr := err
				if isTimeoutError(err) {
					reason = gatewaytypes.CloseReasonPolicyTimeout
					reportErr = gatewaytypes.ErrWriteTimeout
				}
				state.close(reason, reportErr)
				return
			}
			state.touchActivity()
		}
	}()
}

func (s *Server) startIdleMonitor(state *sessionState) {
	if state == nil {
		return
	}

	timeout := s.options.DefaultSession.IdleTimeout
	if timeout <= 0 {
		return
	}

	interval := timeout / 4
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	if interval > 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}

	s.workerWG.Add(1)
	go func() {
		defer s.workerWG.Done()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-state.closedCh:
				return
			case <-ticker.C:
				if state.isClosed() {
					return
				}
				if time.Since(state.lastSeenActivity()) >= timeout {
					state.close(gatewaytypes.CloseReasonIdleTimeout, gatewaytypes.ErrIdleTimeout)
					return
				}
			}
		}
	}()
}

func (s *Server) replyTokens(listener *listenerRuntime, sess session.Session, count int) []string {
	if listener == nil || listener.tracker == nil || count <= 0 {
		return nil
	}
	return listener.tracker.TakeReplyTokens(sess, count)
}

func (s *Server) handleHandlerError(state *sessionState, err error) {
	if state == nil || err == nil {
		return
	}

	reason := closeReasonForError(err, gatewaytypes.CloseReasonHandlerError)
	if s.options.DefaultSession.CloseOnHandlerError {
		state.close(reason, err)
		return
	}
	s.dispatcher.sessionError(state, reason, err)
}

type writeDeadlineConn interface {
	SetWriteDeadline(time.Time) error
}

func (s *Server) writePayload(state *sessionState, payload []byte) error {
	if state == nil || state.conn == nil {
		return session.ErrSessionClosed
	}

	timeout := s.options.DefaultSession.WriteTimeout
	if timeout <= 0 {
		return state.conn.Write(payload)
	}

	if deadlineConn, ok := state.conn.(writeDeadlineConn); ok {
		if err := deadlineConn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		defer func() {
			_ = deadlineConn.SetWriteDeadline(time.Time{})
		}()
		return state.conn.Write(payload)
	}

	done := make(chan error, 1)
	go func() {
		done <- state.conn.Write(payload)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-done:
		return err
	case <-timer.C:
		return gatewaytypes.ErrWriteTimeout
	}
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, gatewaytypes.ErrWriteTimeout) {
		return true
	}

	type timeout interface {
		Timeout() bool
	}
	var te timeout
	return errors.As(err, &te) && te.Timeout()
}

func closeReasonForError(err error, fallback gatewaytypes.CloseReason) gatewaytypes.CloseReason {
	switch {
	case errors.Is(err, session.ErrWriteQueueFull):
		return gatewaytypes.CloseReasonWriteQueueFull
	case errors.Is(err, session.ErrOutboundOverflow):
		return gatewaytypes.CloseReasonOutboundOverflow
	default:
		return fallback
	}
}

func (s *Server) registerState(state *sessionState) {
	if state == nil || state.session == nil {
		return
	}

	s.mu.Lock()
	s.states[state.key] = state
	s.mu.Unlock()
	s.sessions.Add(state.session)
}

func (s *Server) unregisterState(state *sessionState) {
	if state == nil || state.session == nil {
		return
	}

	s.mu.Lock()
	delete(s.states, state.key)
	s.mu.Unlock()
	s.sessions.Remove(state.session.ID())
}

func (s *Server) state(listener string, connID uint64) *sessionState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.states[connKey{listener: listener, connID: connID}]
}

func (s *Server) rollbackStart(listeners []transport.Listener) {
	for i := len(listeners) - 1; i >= 0; i-- {
		_ = listeners[i].Stop()
	}
}

func (s *Server) ListenerAddr(name string) string {
	if s == nil || name == "" {
		return ""
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, runtime := range s.listeners {
		if runtime != nil && runtime.options.Name == name && runtime.listener != nil {
			return runtime.listener.Addr()
		}
	}
	return ""
}

func (st *sessionState) close(reason gatewaytypes.CloseReason, err error) {
	if st == nil || st.server == nil {
		return
	}

	st.closeOnce.Do(func() {
		st.setCloseReason(reason)
		st.server.unregisterState(st)
		if err != nil {
			st.server.dispatcher.sessionError(st, reason, err)
		}
		if st.session != nil {
			_ = st.session.Close()
		}
		if st.listener != nil {
			if closeErr := st.listener.adapter.OnClose(st.session); closeErr != nil {
				st.server.dispatcher.sessionError(st, reason, closeErr)
			}
		}
		if st.conn != nil {
			if closeErr := st.conn.Close(); closeErr != nil {
				st.server.dispatcher.sessionError(st, reason, closeErr)
			}
		}
		_ = st.server.dispatcher.sessionClose(st)
		close(st.closedCh)
	})
}

func (st *sessionState) isClosed() bool {
	if st == nil {
		return true
	}

	select {
	case <-st.closedCh:
		return true
	default:
		return false
	}
}

func (st *sessionState) setCloseReason(reason gatewaytypes.CloseReason) {
	if st == nil {
		return
	}

	st.metaMu.Lock()
	st.closeReasonValue = reason
	st.metaMu.Unlock()
}

func (st *sessionState) closeReason() gatewaytypes.CloseReason {
	if st == nil {
		return ""
	}

	st.metaMu.RLock()
	defer st.metaMu.RUnlock()
	return st.closeReasonValue
}

func (st *sessionState) touchActivity() {
	if st == nil {
		return
	}
	st.lastActivity.Store(time.Now().UnixNano())
}

func (st *sessionState) lastSeenActivity() time.Time {
	if st == nil {
		return time.Time{}
	}

	last := st.lastActivity.Load()
	if last == 0 {
		return time.Time{}
	}
	return time.Unix(0, last)
}
