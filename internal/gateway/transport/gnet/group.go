package gnet

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway/transport"
	gnetv2 "github.com/panjf2000/gnet/v2"
)

var errWebSocketNotImplemented = errors.New("websocket transport is not implemented yet")

type listenerRuntime struct {
	opts    transport.ListenerOptions
	handler transport.ConnHandler

	mu        sync.RWMutex
	boundAddr string
	active    bool
	conns     map[*connState]struct{}
}

func (r *listenerRuntime) addr() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.boundAddr
}

func (r *listenerRuntime) setAddr(addr string) {
	r.mu.Lock()
	r.boundAddr = addr
	r.mu.Unlock()
}

func (r *listenerRuntime) isActive() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.active
}

func (r *listenerRuntime) setActive(active bool) {
	r.mu.Lock()
	r.active = active
	r.mu.Unlock()
}

func (r *listenerRuntime) trackConn(state *connState) {
	if r == nil || state == nil {
		return
	}

	r.mu.Lock()
	if r.conns == nil {
		r.conns = make(map[*connState]struct{})
	}
	r.conns[state] = struct{}{}
	r.mu.Unlock()
}

func (r *listenerRuntime) untrackConn(state *connState) {
	if r == nil || state == nil {
		return
	}

	r.mu.Lock()
	delete(r.conns, state)
	r.mu.Unlock()
}

func (r *listenerRuntime) snapshotConns() []*connState {
	r.mu.RLock()
	defer r.mu.RUnlock()

	conns := make([]*connState, 0, len(r.conns))
	for state := range r.conns {
		conns = append(conns, state)
	}
	return conns
}

type engineCycle struct {
	bootOnce sync.Once
	bootCh   chan error
	doneCh   chan error
}

func newEngineCycle() *engineCycle {
	return &engineCycle{
		bootCh: make(chan error, 1),
		doneCh: make(chan error, 1),
	}
}

func (c *engineCycle) signalBoot(err error) {
	c.bootOnce.Do(func() {
		c.bootCh <- err
		close(c.bootCh)
	})
}

type engineGroup struct {
	gnetv2.BuiltinEventEngine

	mu            sync.Mutex
	runtimes      []*listenerRuntime
	tcpRuntimes   []*listenerRuntime
	routes        map[string]*listenerRuntime
	engine        gnetv2.Engine
	cycle         *engineCycle
	running       bool
	transitioning bool
	transitionCh  chan struct{}
	bootRuntimes  []*listenerRuntime

	nextConnID atomic.Uint64
}

func newEngineGroup(specs []transport.ListenerSpec) *engineGroup {
	runtimes := make([]*listenerRuntime, 0, len(specs))
	tcpRuntimes := make([]*listenerRuntime, 0, len(specs))
	for _, spec := range specs {
		runtime := &listenerRuntime{
			opts:    spec.Options,
			handler: spec.Handler,
			conns:   make(map[*connState]struct{}),
		}
		runtimes = append(runtimes, runtime)
		if spec.Options.Network == "tcp" {
			tcpRuntimes = append(tcpRuntimes, runtime)
		}
	}

	return &engineGroup{
		runtimes:    runtimes,
		tcpRuntimes: tcpRuntimes,
		routes:      make(map[string]*listenerRuntime, len(tcpRuntimes)),
	}
}

func (g *engineGroup) start(runtime *listenerRuntime) error {
	if runtime == nil {
		return fmt.Errorf("gateway/transport/gnet: missing listener runtime")
	}
	if runtime.opts.Network == "websocket" {
		return errWebSocketNotImplemented
	}

	runtime.setActive(true)

	for {
		g.mu.Lock()
		if g.transitioning {
			wait := g.transitionCh
			g.mu.Unlock()
			<-wait
			continue
		}
		if g.running {
			g.mu.Unlock()
			return nil
		}

		g.transitioning = true
		g.transitionCh = make(chan struct{})
		bootRuntimes := append([]*listenerRuntime(nil), g.tcpRuntimes...)
		g.mu.Unlock()

		err := g.startEngine(bootRuntimes)

		g.mu.Lock()
		if err == nil {
			g.running = true
		}
		close(g.transitionCh)
		g.transitioning = false
		g.mu.Unlock()

		if err != nil {
			runtime.setActive(false)
			return err
		}
		return nil
	}
}

func (g *engineGroup) stop(runtime *listenerRuntime) error {
	if runtime == nil {
		return nil
	}
	if runtime.opts.Network == "websocket" {
		runtime.setActive(false)
		return nil
	}

	runtime.setActive(false)
	for _, state := range runtime.snapshotConns() {
		_ = state.raw.Close()
	}

	for {
		g.mu.Lock()
		if g.transitioning {
			wait := g.transitionCh
			g.mu.Unlock()
			<-wait
			continue
		}
		if g.hasActiveTCPLocked() {
			g.mu.Unlock()
			return nil
		}
		if !g.running {
			g.mu.Unlock()
			return nil
		}

		g.transitioning = true
		g.transitionCh = make(chan struct{})
		engine := g.engine
		cycle := g.cycle
		g.mu.Unlock()

		err := g.stopEngine(engine, cycle)

		g.mu.Lock()
		if g.engine == engine && g.cycle == cycle {
			g.engine = gnetv2.Engine{}
			g.cycle = nil
			g.routes = make(map[string]*listenerRuntime, len(g.tcpRuntimes))
			g.bootRuntimes = nil
			g.running = false
		}
		close(g.transitionCh)
		g.transitioning = false
		g.mu.Unlock()
		return err
	}
}

func (g *engineGroup) startEngine(runtimes []*listenerRuntime) error {
	cycle := newEngineCycle()

	g.mu.Lock()
	g.cycle = cycle
	g.bootRuntimes = append([]*listenerRuntime(nil), runtimes...)
	g.routes = make(map[string]*listenerRuntime, len(runtimes))
	g.mu.Unlock()

	addrs := make([]string, 0, len(runtimes))
	for _, runtime := range runtimes {
		addrs = append(addrs, "tcp://"+runtime.opts.Address)
	}

	go func() {
		err := gnetv2.Rotate(g, addrs)
		cycle.signalBoot(err)
		cycle.doneCh <- err
		close(cycle.doneCh)
	}()

	if err := <-cycle.bootCh; err != nil {
		g.mu.Lock()
		if g.cycle == cycle {
			g.cycle = nil
			g.bootRuntimes = nil
			g.routes = make(map[string]*listenerRuntime, len(g.tcpRuntimes))
			g.engine = gnetv2.Engine{}
			g.running = false
		}
		g.mu.Unlock()
		return err
	}

	g.mu.Lock()
	if g.cycle == cycle {
		g.bootRuntimes = nil
	}
	g.mu.Unlock()
	return nil
}

func (g *engineGroup) stopEngine(engine gnetv2.Engine, cycle *engineCycle) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := engine.Stop(ctx)
	cancel()

	if cycle != nil {
		if runErr, ok := <-cycle.doneCh; ok && err == nil {
			err = runErr
		}
	}
	return err
}

func (g *engineGroup) OnBoot(engine gnetv2.Engine) (action gnetv2.Action) {
	g.mu.Lock()
	g.engine = engine
	cycle := g.cycle
	runtimes := append([]*listenerRuntime(nil), g.bootRuntimes...)
	g.mu.Unlock()

	routes, err := g.resolveRoutes(engine, runtimes)
	if err != nil {
		if cycle != nil {
			cycle.signalBoot(err)
		}
		g.reportGroupError(runtimes, err)
		return gnetv2.Shutdown
	}

	g.mu.Lock()
	g.routes = routes
	g.mu.Unlock()

	if cycle != nil {
		cycle.signalBoot(nil)
	}
	return gnetv2.None
}

func (g *engineGroup) OnOpen(c gnetv2.Conn) (out []byte, action gnetv2.Action) {
	runtime := g.runtimeByAddr(c.LocalAddr().String())
	if runtime == nil || !runtime.isActive() {
		return nil, gnetv2.Close
	}

	state := newConnState(g.nextConnID.Add(1), c, runtime)
	runtime.trackConn(state)
	c.SetContext(state)
	state.enqueueOpen()
	return nil, gnetv2.None
}

func (g *engineGroup) OnTraffic(c gnetv2.Conn) (action gnetv2.Action) {
	state, ok := c.Context().(*connState)
	if !ok || state == nil {
		return gnetv2.Close
	}
	if !state.runtime.isActive() {
		_ = c.Close()
		return gnetv2.None
	}

	buf, err := c.Next(-1)
	if err != nil {
		state.enqueueClose(err)
		_ = c.Close()
		return gnetv2.None
	}
	if len(buf) == 0 {
		return gnetv2.None
	}

	payload := append([]byte(nil), buf...)
	state.enqueueData(payload)
	return gnetv2.None
}

func (g *engineGroup) OnClose(c gnetv2.Conn, err error) (action gnetv2.Action) {
	state, _ := c.Context().(*connState)
	if state != nil {
		state.runtime.untrackConn(state)
		state.enqueueClose(err)
	}
	return gnetv2.None
}

func (g *engineGroup) resolveRoutes(engine gnetv2.Engine, runtimes []*listenerRuntime) (map[string]*listenerRuntime, error) {
	routes := make(map[string]*listenerRuntime, len(runtimes))
	for _, runtime := range runtimes {
		addr, err := g.resolveRuntimeAddr(engine, runtime)
		if err != nil {
			return nil, err
		}
		runtime.setAddr(addr)
		routes[runtime.opts.Address] = runtime
		routes[addr] = runtime
	}
	return routes, nil
}

func (g *engineGroup) resolveRuntimeAddr(engine gnetv2.Engine, runtime *listenerRuntime) (string, error) {
	fd, err := engine.DupListener("tcp", runtime.opts.Address)
	if err != nil {
		return "", fmt.Errorf("gateway/transport/gnet: dup listener %q: %w", runtime.opts.Address, err)
	}

	file := os.NewFile(uintptr(fd), runtime.opts.Name)
	if file == nil {
		return "", fmt.Errorf("gateway/transport/gnet: dup listener %q returned nil file", runtime.opts.Address)
	}
	defer file.Close()

	ln, err := net.FileListener(file)
	if err != nil {
		return "", fmt.Errorf("gateway/transport/gnet: resolve listener %q: %w", runtime.opts.Address, err)
	}
	defer ln.Close()

	if ln.Addr() == nil {
		return "", fmt.Errorf("gateway/transport/gnet: listener %q has no bound address", runtime.opts.Address)
	}
	return ln.Addr().String(), nil
}

func (g *engineGroup) runtimeByAddr(addr string) *listenerRuntime {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.routes[addr]
}

func (g *engineGroup) reportGroupError(runtimes []*listenerRuntime, err error) {
	if err == nil {
		return
	}
	for _, runtime := range runtimes {
		if runtime.opts.OnError != nil {
			runtime.opts.OnError(err)
		}
	}
}

func (g *engineGroup) hasActiveTCPLocked() bool {
	for _, runtime := range g.tcpRuntimes {
		if runtime.isActive() {
			return true
		}
	}
	return false
}
