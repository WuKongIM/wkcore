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

	mu               sync.Mutex
	runtimes         []*listenerRuntime
	routes           map[string]*listenerRuntime
	engine           gnetv2.Engine
	cycle            *engineCycle
	running          bool
	reconciling      bool
	reconcileCh      chan struct{}
	startingRuntimes []*listenerRuntime

	nextConnID atomic.Uint64
}

func newEngineGroup(specs []transport.ListenerSpec) *engineGroup {
	runtimes := make([]*listenerRuntime, 0, len(specs))
	for _, spec := range specs {
		runtimes = append(runtimes, &listenerRuntime{
			opts:    spec.Options,
			handler: spec.Handler,
		})
	}

	return &engineGroup{
		runtimes: runtimes,
		routes:   make(map[string]*listenerRuntime, len(runtimes)),
	}
}

func (g *engineGroup) start(runtime *listenerRuntime) error {
	if runtime == nil {
		return fmt.Errorf("gateway/transport/gnet: missing listener runtime")
	}
	if runtime.opts.Network == "websocket" {
		return errWebSocketNotImplemented
	}

	return g.setRuntimeActive(runtime, true)
}

func (g *engineGroup) stop(runtime *listenerRuntime) error {
	if runtime == nil {
		return nil
	}

	return g.setRuntimeActive(runtime, false)
}

func (g *engineGroup) setRuntimeActive(runtime *listenerRuntime, active bool) error {
	for {
		g.mu.Lock()
		if g.reconciling {
			wait := g.reconcileCh
			g.mu.Unlock()
			<-wait
			continue
		}
		if runtime.active == active {
			g.mu.Unlock()
			return nil
		}

		previous := g.activeRuntimesLocked()
		runtime.active = active
		desired := g.activeRuntimesLocked()
		if sameRuntimes(previous, desired) {
			g.mu.Unlock()
			return nil
		}

		g.reconciling = true
		g.reconcileCh = make(chan struct{})
		g.mu.Unlock()

		err := g.reconcileActiveRuntimes(desired)
		if err != nil {
			rollbackErr := g.reconcileActiveRuntimes(previous)

			g.mu.Lock()
			g.setActiveRuntimesLocked(previous)
			close(g.reconcileCh)
			g.reconciling = false
			g.mu.Unlock()

			if rollbackErr != nil {
				return fmt.Errorf("gateway/transport/gnet: reconcile failed: %w (rollback failed: %v)", err, rollbackErr)
			}
			return err
		}

		g.mu.Lock()
		close(g.reconcileCh)
		g.reconciling = false
		g.mu.Unlock()
		return nil
	}
}

func (g *engineGroup) reconcileActiveRuntimes(desired []*listenerRuntime) error {
	if err := g.stopEngine(); err != nil {
		return err
	}
	if len(desired) == 0 {
		return nil
	}
	return g.startEngine(desired)
}

func (g *engineGroup) startEngine(runtimes []*listenerRuntime) error {
	cycle := newEngineCycle()

	g.mu.Lock()
	g.cycle = cycle
	g.routes = make(map[string]*listenerRuntime, len(runtimes))
	g.startingRuntimes = append([]*listenerRuntime(nil), runtimes...)
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
			g.startingRuntimes = nil
			g.routes = make(map[string]*listenerRuntime)
			g.engine = gnetv2.Engine{}
			g.running = false
		}
		g.mu.Unlock()
		return err
	}

	g.mu.Lock()
	if g.cycle == cycle {
		g.running = true
		g.startingRuntimes = nil
	}
	g.mu.Unlock()
	return nil
}

func (g *engineGroup) OnBoot(engine gnetv2.Engine) (action gnetv2.Action) {
	g.mu.Lock()
	g.engine = engine
	cycle := g.cycle
	runtimes := append([]*listenerRuntime(nil), g.startingRuntimes...)
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
	if runtime == nil {
		return nil, gnetv2.Close
	}

	state := newConnState(g.nextConnID.Add(1), c, runtime)
	c.SetContext(state)
	state.enqueueOpen()
	return nil, gnetv2.None
}

func (g *engineGroup) OnTraffic(c gnetv2.Conn) (action gnetv2.Action) {
	state, ok := c.Context().(*connState)
	if !ok || state == nil {
		return gnetv2.Close
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

func (g *engineGroup) stopEngine() error {
	g.mu.Lock()
	if !g.running {
		g.cycle = nil
		g.engine = gnetv2.Engine{}
		g.routes = make(map[string]*listenerRuntime)
		g.startingRuntimes = nil
		g.mu.Unlock()
		return nil
	}

	engine := g.engine
	cycle := g.cycle
	g.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := engine.Stop(ctx)
	cancel()

	if cycle != nil {
		if runErr, ok := <-cycle.doneCh; ok && err == nil {
			err = runErr
		}
	}

	g.mu.Lock()
	if g.cycle == cycle {
		g.cycle = nil
		g.engine = gnetv2.Engine{}
		g.routes = make(map[string]*listenerRuntime)
		g.startingRuntimes = nil
		g.running = false
	}
	g.mu.Unlock()

	return err
}

func (g *engineGroup) activeRuntimesLocked() []*listenerRuntime {
	active := make([]*listenerRuntime, 0, len(g.runtimes))
	for _, runtime := range g.runtimes {
		if runtime.active {
			active = append(active, runtime)
		}
	}
	return active
}

func (g *engineGroup) setActiveRuntimesLocked(active []*listenerRuntime) {
	activeSet := make(map[*listenerRuntime]struct{}, len(active))
	for _, runtime := range active {
		activeSet[runtime] = struct{}{}
	}
	for _, runtime := range g.runtimes {
		_, ok := activeSet[runtime]
		runtime.active = ok
	}
}

func sameRuntimes(left, right []*listenerRuntime) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
