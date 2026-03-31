package core_test

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/gateway/core"
	"github.com/WuKongIM/WuKongIM/internal/gateway/session"
	"github.com/WuKongIM/WuKongIM/internal/gateway/testkit"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

func TestServer(t *testing.T) {
	t.Run("decoded frames are delivered to the handler", func(t *testing.T) {
		handler := newTestHandler()
		proto := newScriptedProtocol("fake-proto")
		proto.pushDecode(decodeResult{
			frames:     []wkpacket.Frame{&wkpacket.PingPacket{}},
			consumed:   1,
			tokenBatch: []string{""},
		})

		srv, transportFactory := newTestServer(t, handler, proto, gateway.SessionOptions{})
		if err := srv.Start(); err != nil {
			t.Fatalf("start failed: %v", err)
		}
		t.Cleanup(func() { _ = srv.Stop() })

		transportFactory.MustOpen("listener-a", 1)
		transportFactory.MustData("listener-a", 1, []byte("x"))

		waitFor(t, func() bool { return handler.frameCount() == 1 })
		if _, ok := handler.frames()[0].(*wkpacket.PingPacket); !ok {
			t.Fatalf("expected ping packet, got %T", handler.frames()[0])
		}
	})

	t.Run("listener scoped errors go to OnListenerError before a session exists", func(t *testing.T) {
		handler := newTestHandler()
		proto := newScriptedProtocol("fake-proto")
		srv, transportFactory := newTestServer(t, handler, proto, gateway.SessionOptions{})
		if err := srv.Start(); err != nil {
			t.Fatalf("start failed: %v", err)
		}
		t.Cleanup(func() { _ = srv.Stop() })

		listenerErr := errors.New("listener boom")
		transportFactory.MustError("listener-a", listenerErr)

		waitFor(t, func() bool { return len(handler.listenerErrors()) == 1 })
		got := handler.listenerErrors()[0]
		if got.Listener != "listener-a" {
			t.Fatalf("expected listener-a, got %q", got.Listener)
		}
		if !errors.Is(got.Err, listenerErr) {
			t.Fatalf("expected %v, got %v", listenerErr, got.Err)
		}
		if len(handler.sessionErrors()) != 0 {
			t.Fatalf("expected no session errors, got %d", len(handler.sessionErrors()))
		}
	})

	t.Run("partial inbound data does not dispatch until a complete frame is available", func(t *testing.T) {
		handler := newTestHandler()
		proto := newScriptedProtocol("fake-proto")
		proto.pushDecode(decodeResult{})
		proto.pushDecode(decodeResult{
			frames:   []wkpacket.Frame{&wkpacket.PingPacket{}},
			consumed: 2,
		})

		srv, transportFactory := newTestServer(t, handler, proto, gateway.SessionOptions{})
		if err := srv.Start(); err != nil {
			t.Fatalf("start failed: %v", err)
		}
		t.Cleanup(func() { _ = srv.Stop() })

		transportFactory.MustOpen("listener-a", 1)
		transportFactory.MustData("listener-a", 1, []byte("a"))
		if got := handler.frameCount(); got != 0 {
			t.Fatalf("expected no frames after partial data, got %d", got)
		}

		transportFactory.MustData("listener-a", 1, []byte("b"))
		waitFor(t, func() bool { return handler.frameCount() == 1 })
	})

	t.Run("inbound overflow closes with CloseReasonInboundOverflow", func(t *testing.T) {
		handler := newTestHandler()
		proto := newScriptedProtocol("fake-proto")
		srv, transportFactory := newTestServer(t, handler, proto, gateway.SessionOptions{
			MaxInboundBytes: 1,
		})
		if err := srv.Start(); err != nil {
			t.Fatalf("start failed: %v", err)
		}
		t.Cleanup(func() { _ = srv.Stop() })

		transportFactory.MustOpen("listener-a", 1)
		transportFactory.MustData("listener-a", 1, []byte("ab"))

		waitFor(t, func() bool { return handler.closeCount() == 1 })
		if got := handler.closeReasons()[0]; got != gateway.CloseReasonInboundOverflow {
			t.Fatalf("expected %q, got %q", gateway.CloseReasonInboundOverflow, got)
		}
		if len(handler.sessionErrors()) != 1 {
			t.Fatalf("expected one session error, got %d", len(handler.sessionErrors()))
		}
		if !reflect.DeepEqual(handler.callOrder(), []string{"open", "error", "close"}) {
			t.Fatalf("unexpected call order: %v", handler.callOrder())
		}
	})

	t.Run("handler error closes when CloseOnHandlerError is true", func(t *testing.T) {
		handlerErr := errors.New("handler boom")
		handler := newTestHandler()
		handler.onFrame = func(*gateway.Context, wkpacket.Frame) error { return handlerErr }

		proto := newScriptedProtocol("fake-proto")
		proto.pushDecode(decodeResult{
			frames:   []wkpacket.Frame{&wkpacket.PingPacket{}},
			consumed: 1,
		})

		srv, transportFactory := newTestServer(t, handler, proto, gateway.SessionOptions{
			CloseOnHandlerError: true,
		})
		if err := srv.Start(); err != nil {
			t.Fatalf("start failed: %v", err)
		}
		t.Cleanup(func() { _ = srv.Stop() })

		transportFactory.MustOpen("listener-a", 1)
		transportFactory.MustData("listener-a", 1, []byte("x"))

		waitFor(t, func() bool { return handler.closeCount() == 1 })
		reasons := handler.closeReasons()
		if got := reasons[len(reasons)-1]; got != gateway.CloseReasonHandlerError {
			t.Fatalf("expected %q, got %q", gateway.CloseReasonHandlerError, got)
		}
		if len(handler.sessionErrors()) != 1 || !errors.Is(handler.sessionErrors()[0], handlerErr) {
			t.Fatalf("expected session error %v, got %v", handlerErr, handler.sessionErrors())
		}
	})

	t.Run("reply token from protocol decode is visible in handler context", func(t *testing.T) {
		handler := newTestHandler()
		proto := newScriptedProtocol("fake-proto")
		proto.pushDecode(decodeResult{
			frames:     []wkpacket.Frame{&wkpacket.PingPacket{}},
			consumed:   1,
			tokenBatch: []string{"reply-1"},
		})

		srv, transportFactory := newTestServer(t, handler, proto, gateway.SessionOptions{})
		if err := srv.Start(); err != nil {
			t.Fatalf("start failed: %v", err)
		}
		t.Cleanup(func() { _ = srv.Stop() })

		transportFactory.MustOpen("listener-a", 1)
		transportFactory.MustData("listener-a", 1, []byte("x"))

		waitFor(t, func() bool { return handler.frameCount() == 1 })
		if got := handler.contexts()[1].ReplyToken; got != "reply-1" {
			t.Fatalf("expected reply token reply-1, got %q", got)
		}
	})

	t.Run("protocol decode failure closes with CloseReasonProtocolError", func(t *testing.T) {
		decodeErr := errors.New("decode boom")
		handler := newTestHandler()
		proto := newScriptedProtocol("fake-proto")
		proto.pushDecode(decodeResult{err: decodeErr})

		srv, transportFactory := newTestServer(t, handler, proto, gateway.SessionOptions{})
		if err := srv.Start(); err != nil {
			t.Fatalf("start failed: %v", err)
		}
		t.Cleanup(func() { _ = srv.Stop() })

		transportFactory.MustOpen("listener-a", 1)
		transportFactory.MustData("listener-a", 1, []byte("x"))

		waitFor(t, func() bool { return handler.closeCount() == 1 })
		if got := handler.closeReasons()[0]; got != gateway.CloseReasonProtocolError {
			t.Fatalf("expected %q, got %q", gateway.CloseReasonProtocolError, got)
		}
	})

	t.Run("OnSessionError fires before close on protocol decode failure", func(t *testing.T) {
		handler := newTestHandler()
		proto := newScriptedProtocol("fake-proto")
		proto.pushDecode(decodeResult{err: errors.New("decode boom")})

		srv, transportFactory := newTestServer(t, handler, proto, gateway.SessionOptions{})
		if err := srv.Start(); err != nil {
			t.Fatalf("start failed: %v", err)
		}
		t.Cleanup(func() { _ = srv.Stop() })

		transportFactory.MustOpen("listener-a", 1)
		transportFactory.MustData("listener-a", 1, []byte("x"))

		waitFor(t, func() bool { return handler.closeCount() == 1 })
		if !reflect.DeepEqual(handler.callOrder(), []string{"open", "error", "close"}) {
			t.Fatalf("unexpected call order: %v", handler.callOrder())
		}
	})

	t.Run("OnSessionError fires before close on queue full and outbound overflow", func(t *testing.T) {
		t.Run("queue full", func(t *testing.T) {
			handler := newTestHandler()
			handler.onOpen = func(ctx *gateway.Context) error {
				if err := ctx.WriteFrame(&wkpacket.PingPacket{}); err != nil {
					return err
				}
				return ctx.WriteFrame(&wkpacket.PingPacket{})
			}

			proto := newScriptedProtocol("fake-proto")
			proto.encodedBytes = []byte("x")

			srv, transportFactory := newTestServer(t, handler, proto, gateway.SessionOptions{
				WriteQueueSize:   1,
				MaxOutboundBytes: 16,
			})
			if err := srv.Start(); err != nil {
				t.Fatalf("start failed: %v", err)
			}
			t.Cleanup(func() { _ = srv.Stop() })

			transportFactory.MustOpen("listener-a", 1)

			waitFor(t, func() bool { return handler.closeCount() == 1 })
			if !errors.Is(handler.sessionErrors()[0], session.ErrWriteQueueFull) {
				t.Fatalf("expected queue full error, got %v", handler.sessionErrors())
			}
			if got := handler.closeReasons()[0]; got != gateway.CloseReasonWriteQueueFull {
				t.Fatalf("expected %q, got %q", gateway.CloseReasonWriteQueueFull, got)
			}
			if !reflect.DeepEqual(handler.callOrder(), []string{"open", "error", "close"}) {
				t.Fatalf("unexpected call order: %v", handler.callOrder())
			}
		})

		t.Run("outbound overflow", func(t *testing.T) {
			handler := newTestHandler()
			handler.onOpen = func(ctx *gateway.Context) error {
				return ctx.WriteFrame(&wkpacket.PingPacket{})
			}

			proto := newScriptedProtocol("fake-proto")
			proto.encodedBytes = []byte("xx")

			srv, transportFactory := newTestServer(t, handler, proto, gateway.SessionOptions{
				WriteQueueSize:   1,
				MaxOutboundBytes: 1,
			})
			if err := srv.Start(); err != nil {
				t.Fatalf("start failed: %v", err)
			}
			t.Cleanup(func() { _ = srv.Stop() })

			transportFactory.MustOpen("listener-a", 1)

			waitFor(t, func() bool { return handler.closeCount() == 1 })
			if !errors.Is(handler.sessionErrors()[0], session.ErrOutboundOverflow) {
				t.Fatalf("expected outbound overflow error, got %v", handler.sessionErrors())
			}
			if got := handler.closeReasons()[0]; got != gateway.CloseReasonOutboundOverflow {
				t.Fatalf("expected %q, got %q", gateway.CloseReasonOutboundOverflow, got)
			}
			if !reflect.DeepEqual(handler.callOrder(), []string{"open", "error", "close"}) {
				t.Fatalf("unexpected call order: %v", handler.callOrder())
			}
		})
	})

	t.Run("peer close maps to CloseReasonPeerClosed", func(t *testing.T) {
		handler := newTestHandler()
		proto := newScriptedProtocol("fake-proto")
		srv, transportFactory := newTestServer(t, handler, proto, gateway.SessionOptions{})
		if err := srv.Start(); err != nil {
			t.Fatalf("start failed: %v", err)
		}
		t.Cleanup(func() { _ = srv.Stop() })

		transportFactory.MustOpen("listener-a", 1)
		transportFactory.MustClose("listener-a", 1, nil)

		waitFor(t, func() bool { return handler.closeCount() == 1 })
		if got := handler.closeReasons()[0]; got != gateway.CloseReasonPeerClosed {
			t.Fatalf("expected %q, got %q", gateway.CloseReasonPeerClosed, got)
		}
	})

	t.Run("Stop maps active sessions to CloseReasonServerStop", func(t *testing.T) {
		handler := newTestHandler()
		proto := newScriptedProtocol("fake-proto")
		srv, transportFactory := newTestServer(t, handler, proto, gateway.SessionOptions{})
		if err := srv.Start(); err != nil {
			t.Fatalf("start failed: %v", err)
		}

		transportFactory.MustOpen("listener-a", 1)
		if err := srv.Stop(); err != nil {
			t.Fatalf("stop failed: %v", err)
		}

		waitFor(t, func() bool { return handler.closeCount() == 1 })
		if got := handler.closeReasons()[0]; got != gateway.CloseReasonServerStop {
			t.Fatalf("expected %q, got %q", gateway.CloseReasonServerStop, got)
		}
	})

	t.Run("close callback fires once", func(t *testing.T) {
		handler := newTestHandler()
		proto := newScriptedProtocol("fake-proto")
		srv, transportFactory := newTestServer(t, handler, proto, gateway.SessionOptions{})
		if err := srv.Start(); err != nil {
			t.Fatalf("start failed: %v", err)
		}

		transportFactory.MustOpen("listener-a", 1)
		transportFactory.MustClose("listener-a", 1, nil)
		waitFor(t, func() bool { return handler.closeCount() == 1 })

		if err := srv.Stop(); err != nil {
			t.Fatalf("stop failed: %v", err)
		}

		if got := handler.closeCount(); got != 1 {
			t.Fatalf("expected close callback once, got %d", got)
		}
		if proto.onCloseCalls() != 1 {
			t.Fatalf("expected protocol OnClose once, got %d", proto.onCloseCalls())
		}
	})
}

func TestIdleTimeout(t *testing.T) {
	handler := newTestHandler()
	proto := newScriptedProtocol("fake-proto")

	srv, transportFactory := newTestServer(t, handler, proto, gateway.SessionOptions{
		IdleTimeout: 30 * time.Millisecond,
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })

	transportFactory.MustOpen("listener-a", 1)

	waitFor(t, func() bool { return handler.closeCount() == 1 })
	reasons := handler.closeReasons()
	if got := reasons[len(reasons)-1]; got != gateway.CloseReasonIdleTimeout {
		t.Fatalf("expected %q, got %q", gateway.CloseReasonIdleTimeout, got)
	}
	if len(handler.sessionErrors()) != 1 {
		t.Fatalf("expected one session error, got %d", len(handler.sessionErrors()))
	}
}

func TestWriteTimeout(t *testing.T) {
	handler := newTestHandler()
	handler.onFrame = func(ctx *gateway.Context, frame wkpacket.Frame) error {
		return ctx.WriteFrame(&wkpacket.PingPacket{})
	}

	proto := newScriptedProtocol("fake-proto")
	proto.encodedBytes = []byte("x")
	proto.pushDecode(decodeResult{
		frames:   []wkpacket.Frame{&wkpacket.PingPacket{}},
		consumed: 1,
	})

	srv, transportFactory := newTestServer(t, handler, proto, gateway.SessionOptions{
		WriteTimeout: 30 * time.Millisecond,
	})
	if err := srv.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop() })

	conn := transportFactory.MustOpen("listener-a", 1)
	conn.BlockWrites()
	transportFactory.MustData("listener-a", 1, []byte("x"))

	waitFor(t, func() bool { return handler.closeCount() == 1 })
	reasons := handler.closeReasons()
	if got := reasons[len(reasons)-1]; got != gateway.CloseReasonPolicyTimeout {
		t.Fatalf("expected %q, got %q", gateway.CloseReasonPolicyTimeout, got)
	}
	if len(handler.sessionErrors()) != 1 {
		t.Fatalf("expected one session error, got %d", len(handler.sessionErrors()))
	}
	if !errors.Is(handler.sessionErrors()[0], gateway.ErrWriteTimeout) {
		t.Fatalf("expected write timeout error, got %v", handler.sessionErrors()[0])
	}
}

func newTestServer(t *testing.T, handler *testHandler, proto *scriptedProtocol, sessOpts gateway.SessionOptions) (*core.Server, *testkit.FakeTransportFactory) {
	t.Helper()

	registry := core.NewRegistry()
	transportFactory := testkit.NewFakeTransportFactory("fake-transport")
	if err := registry.RegisterTransport(transportFactory); err != nil {
		t.Fatalf("register transport failed: %v", err)
	}
	if err := registry.RegisterProtocol(proto); err != nil {
		t.Fatalf("register protocol failed: %v", err)
	}

	srv, err := core.NewServer(registry, &gateway.Options{
		Handler:        handler,
		DefaultSession: sessOpts,
		Listeners: []gateway.ListenerOptions{
			{
				Name:      "listener-a",
				Network:   "tcp",
				Address:   "127.0.0.1:9000",
				Transport: transportFactory.Name(),
				Protocol:  proto.Name(),
			},
		},
	})
	if err != nil {
		t.Fatalf("new server failed: %v", err)
	}
	return srv, transportFactory
}

type decodeResult struct {
	frames     []wkpacket.Frame
	consumed   int
	err        error
	tokenBatch []string
}

type scriptedProtocol struct {
	name string

	mu           sync.Mutex
	decodeQueue  []decodeResult
	tokenQueue   [][]string
	encodedBytes []byte
	encodeErr    error
	openErr      error
	closeErr     error
	closeCalls   int
}

func newScriptedProtocol(name string) *scriptedProtocol {
	return &scriptedProtocol{name: name}
}

func (p *scriptedProtocol) Name() string { return p.name }

func (p *scriptedProtocol) Decode(_ session.Session, _ []byte) ([]wkpacket.Frame, int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.decodeQueue) == 0 {
		return nil, 0, nil
	}
	step := p.decodeQueue[0]
	p.decodeQueue = p.decodeQueue[1:]
	if len(step.tokenBatch) > 0 {
		p.tokenQueue = append(p.tokenQueue, append([]string(nil), step.tokenBatch...))
	}
	return append([]wkpacket.Frame(nil), step.frames...), step.consumed, step.err
}

func (p *scriptedProtocol) Encode(_ session.Session, _ wkpacket.Frame, _ session.OutboundMeta) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.encodeErr != nil {
		return nil, p.encodeErr
	}
	return append([]byte(nil), p.encodedBytes...), nil
}

func (p *scriptedProtocol) OnOpen(session.Session) error { return p.openErr }

func (p *scriptedProtocol) OnClose(session.Session) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closeCalls++
	return p.closeErr
}

func (p *scriptedProtocol) TakeReplyTokens(session.Session, int) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.tokenQueue) == 0 {
		return nil
	}
	tokens := append([]string(nil), p.tokenQueue[0]...)
	p.tokenQueue = p.tokenQueue[1:]
	return tokens
}

func (p *scriptedProtocol) pushDecode(step decodeResult) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.decodeQueue = append(p.decodeQueue, step)
}

func (p *scriptedProtocol) onCloseCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closeCalls
}

type testHandler struct {
	mu sync.Mutex

	order          []string
	listenerErrs   []testkit.ListenerError
	sessionErrs    []error
	closeReasonLog []gateway.CloseReason
	framesSeen     []wkpacket.Frame
	contextCopies  []gateway.Context

	onOpen  func(*gateway.Context) error
	onFrame func(*gateway.Context, wkpacket.Frame) error
	onClose func(*gateway.Context) error
}

func newTestHandler() *testHandler { return &testHandler{} }

func (h *testHandler) OnListenerError(listener string, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.order = append(h.order, "listener_error")
	h.listenerErrs = append(h.listenerErrs, testkit.ListenerError{Listener: listener, Err: err})
}

func (h *testHandler) OnSessionOpen(ctx *gateway.Context) error {
	h.mu.Lock()
	h.order = append(h.order, "open")
	if ctx != nil {
		h.contextCopies = append(h.contextCopies, *ctx)
	}
	fn := h.onOpen
	h.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	return nil
}

func (h *testHandler) OnFrame(ctx *gateway.Context, frame wkpacket.Frame) error {
	h.mu.Lock()
	h.order = append(h.order, "frame")
	if ctx != nil {
		h.contextCopies = append(h.contextCopies, *ctx)
		h.closeReasonLog = append(h.closeReasonLog, ctx.CloseReason)
	}
	h.framesSeen = append(h.framesSeen, frame)
	fn := h.onFrame
	h.mu.Unlock()
	if fn != nil {
		return fn(ctx, frame)
	}
	return nil
}

func (h *testHandler) OnSessionClose(ctx *gateway.Context) error {
	h.mu.Lock()
	h.order = append(h.order, "close")
	if ctx != nil {
		h.contextCopies = append(h.contextCopies, *ctx)
		h.closeReasonLog = append(h.closeReasonLog, ctx.CloseReason)
	}
	fn := h.onClose
	h.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	return nil
}

func (h *testHandler) OnSessionError(ctx *gateway.Context, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.order = append(h.order, "error")
	if ctx != nil {
		h.contextCopies = append(h.contextCopies, *ctx)
		h.closeReasonLog = append(h.closeReasonLog, ctx.CloseReason)
	}
	h.sessionErrs = append(h.sessionErrs, err)
}

func (h *testHandler) frameCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.framesSeen)
}

func (h *testHandler) closeCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	count := 0
	for _, call := range h.order {
		if call == "close" {
			count++
		}
	}
	return count
}

func (h *testHandler) frames() []wkpacket.Frame {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]wkpacket.Frame(nil), h.framesSeen...)
}

func (h *testHandler) closeReasons() []gateway.CloseReason {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]gateway.CloseReason(nil), h.closeReasonLog...)
}

func (h *testHandler) contexts() []gateway.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]gateway.Context(nil), h.contextCopies...)
}

func (h *testHandler) callOrder() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.order...)
}

func (h *testHandler) listenerErrors() []testkit.ListenerError {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]testkit.ListenerError(nil), h.listenerErrs...)
}

func (h *testHandler) sessionErrors() []error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]error(nil), h.sessionErrs...)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not satisfied before timeout")
}
