package testkit

import (
	"fmt"
	"sync"

	"github.com/WuKongIM/WuKongIM/internal/gateway/transport"
)

type FakeTransportFactory struct {
	name string

	mu        sync.Mutex
	listeners map[string]*FakeListener
	NewErr    error
}

func NewFakeTransportFactory(name string) *FakeTransportFactory {
	return &FakeTransportFactory{
		name:      name,
		listeners: make(map[string]*FakeListener),
	}
}

func (f *FakeTransportFactory) Name() string {
	if f == nil {
		return ""
	}
	return f.name
}

func (f *FakeTransportFactory) New(opts transport.ListenerOptions, handler transport.ConnHandler) (transport.Listener, error) {
	if f == nil {
		return nil, nil
	}
	if f.NewErr != nil {
		return nil, f.NewErr
	}

	listener := &FakeListener{
		opts:    opts,
		handler: handler,
		addr:    opts.Address,
		conns:   make(map[uint64]*FakeConn),
	}

	f.mu.Lock()
	f.listeners[opts.Name] = listener
	f.mu.Unlock()
	return listener, nil
}

func (f *FakeTransportFactory) Listener(name string) *FakeListener {
	if f == nil {
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listeners[name]
}

func (f *FakeTransportFactory) MustListener(name string) *FakeListener {
	listener := f.Listener(name)
	if listener == nil {
		panic(fmt.Sprintf("fake listener %q not found", name))
	}
	return listener
}

func (f *FakeTransportFactory) MustOpen(listenerName string, connID uint64) *FakeConn {
	return f.MustListener(listenerName).MustOpen(connID)
}

func (f *FakeTransportFactory) MustData(listenerName string, connID uint64, data []byte) {
	if err := f.MustListener(listenerName).EmitData(connID, data); err != nil {
		panic(err)
	}
}

func (f *FakeTransportFactory) MustClose(listenerName string, connID uint64, err error) {
	f.MustListener(listenerName).EmitClose(connID, err)
}

func (f *FakeTransportFactory) MustError(listenerName string, err error) {
	f.MustListener(listenerName).EmitError(err)
}

type FakeListener struct {
	opts    transport.ListenerOptions
	handler transport.ConnHandler

	mu       sync.Mutex
	addr     string
	started  bool
	stopped  bool
	StartErr error
	StopErr  error
	conns    map[uint64]*FakeConn
}

func (l *FakeListener) Start() error {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.started = true
	return l.StartErr
}

func (l *FakeListener) Stop() error {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.stopped = true
	return l.StopErr
}

func (l *FakeListener) Addr() string {
	if l == nil {
		return ""
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.addr != "" {
		return l.addr
	}
	return l.opts.Address
}

func (l *FakeListener) EmitError(err error) {
	if l == nil || l.opts.OnError == nil {
		return
	}
	l.opts.OnError(err)
}

func (l *FakeListener) MustOpen(connID uint64) *FakeConn {
	if l == nil {
		panic("nil fake listener")
	}

	conn := &FakeConn{
		id:         connID,
		listener:   l,
		localAddr:  l.Addr(),
		remoteAddr: fmt.Sprintf("fake-remote-%d", connID),
	}

	l.mu.Lock()
	l.conns[connID] = conn
	handler := l.handler
	l.mu.Unlock()

	if handler != nil {
		if err := handler.OnOpen(conn); err != nil {
			panic(err)
		}
	}

	return conn
}

func (l *FakeListener) EmitData(connID uint64, data []byte) error {
	conn := l.Conn(connID)
	if conn == nil {
		return fmt.Errorf("fake listener: conn %d not found", connID)
	}
	if conn.listener.handler == nil {
		return nil
	}
	return conn.listener.handler.OnData(conn, data)
}

func (l *FakeListener) EmitClose(connID uint64, err error) {
	conn := l.Conn(connID)
	if conn == nil || conn.listener.handler == nil {
		return
	}
	conn.listener.handler.OnClose(conn, err)
}

func (l *FakeListener) Conn(connID uint64) *FakeConn {
	if l == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	return l.conns[connID]
}

func (l *FakeListener) Started() bool {
	if l == nil {
		return false
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	return l.started
}

func (l *FakeListener) Stopped() bool {
	if l == nil {
		return false
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	return l.stopped
}

func (l *FakeListener) SetAddr(addr string) {
	if l == nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.addr = addr
}

type FakeConn struct {
	id         uint64
	listener   *FakeListener
	localAddr  string
	remoteAddr string

	mu       sync.Mutex
	writes   [][]byte
	writeErr error
	closeErr error
	closed   bool
}

func (c *FakeConn) ID() uint64 {
	if c == nil {
		return 0
	}
	return c.id
}

func (c *FakeConn) Write(data []byte) error {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.writes = append(c.writes, append([]byte(nil), data...))
	if c.writeErr != nil {
		return c.writeErr
	}
	return nil
}

func (c *FakeConn) Close() error {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
	return c.closeErr
}

func (c *FakeConn) LocalAddr() string {
	if c == nil {
		return ""
	}
	return c.localAddr
}

func (c *FakeConn) RemoteAddr() string {
	if c == nil {
		return ""
	}
	return c.remoteAddr
}

func (c *FakeConn) Writes() [][]byte {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([][]byte, len(c.writes))
	for i := range c.writes {
		out[i] = append([]byte(nil), c.writes[i]...)
	}
	return out
}

func (c *FakeConn) SetWriteErr(err error) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.writeErr = err
}

func (c *FakeConn) SetCloseErr(err error) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.closeErr = err
}

func (c *FakeConn) EmitData(data []byte) error {
	if c == nil || c.listener == nil || c.listener.handler == nil {
		return nil
	}
	return c.listener.handler.OnData(c, data)
}

func (c *FakeConn) EmitClose(err error) {
	if c == nil || c.listener == nil || c.listener.handler == nil {
		return
	}
	c.listener.handler.OnClose(c, err)
}

func (c *FakeConn) EmitError(err error) {
	if c == nil || c.listener == nil {
		return
	}
	c.listener.EmitError(err)
}
