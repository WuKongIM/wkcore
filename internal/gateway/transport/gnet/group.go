package gnet

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway/transport"
	gnetv2 "github.com/panjf2000/gnet/v2"
)

type listenerRuntime struct {
	opts    transport.ListenerOptions
	handler transport.ConnHandler

	mu        sync.RWMutex
	boundAddr string
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

	mu       sync.Mutex
	runtimes []*listenerRuntime
	routes   map[string]*listenerRuntime
	engine   gnetv2.Engine
	cycle    *engineCycle
	refs     int
	running  bool
	starting bool
	startErr error
	startCh  chan struct{}

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

func (g *engineGroup) start() error {
	for {
		g.mu.Lock()
		if g.running {
			g.refs++
			g.mu.Unlock()
			return nil
		}
		if g.starting {
			wait := g.startCh
			g.mu.Unlock()
			<-wait
			continue
		}

		g.starting = true
		g.startErr = nil
		g.startCh = make(chan struct{})
		g.mu.Unlock()

		err := g.startEngine()

		g.mu.Lock()
		g.startErr = err
		if err == nil {
			g.running = true
			g.refs = 1
		}
		g.starting = false
		close(g.startCh)
		g.mu.Unlock()
		return err
	}
}

func (g *engineGroup) stop() error {
	for {
		g.mu.Lock()
		if g.starting {
			wait := g.startCh
			g.mu.Unlock()
			<-wait
			continue
		}
		if g.refs == 0 {
			g.mu.Unlock()
			return nil
		}

		g.refs--
		if g.refs > 0 {
			g.mu.Unlock()
			return nil
		}
		if !g.running {
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
		if g.refs == 0 {
			g.running = false
			g.engine = gnetv2.Engine{}
			if g.cycle == cycle {
				g.cycle = nil
			}
		}
		g.mu.Unlock()

		return err
	}
}

func (g *engineGroup) startEngine() error {
	cycle := newEngineCycle()

	g.mu.Lock()
	g.cycle = cycle
	g.mu.Unlock()

	addrs := make([]string, 0, len(g.runtimes))
	for _, runtime := range g.runtimes {
		addrs = append(addrs, "tcp://"+runtime.opts.Address)
	}

	go func() {
		err := gnetv2.Rotate(g, addrs)
		cycle.signalBoot(err)
		cycle.doneCh <- err
		close(cycle.doneCh)
	}()

	return <-cycle.bootCh
}

func (g *engineGroup) OnBoot(engine gnetv2.Engine) (action gnetv2.Action) {
	g.mu.Lock()
	g.engine = engine
	cycle := g.cycle
	g.mu.Unlock()

	routes, err := g.resolveRoutes(engine)
	if err != nil {
		if cycle != nil {
			cycle.signalBoot(err)
		}
		g.reportGroupError(err)
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

func (g *engineGroup) resolveRoutes(engine gnetv2.Engine) (map[string]*listenerRuntime, error) {
	routes := make(map[string]*listenerRuntime, len(g.runtimes))
	for _, runtime := range g.runtimes {
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

func (g *engineGroup) reportGroupError(err error) {
	if err == nil {
		return
	}
	for _, runtime := range g.runtimes {
		if runtime.opts.OnError != nil {
			runtime.opts.OnError(err)
		}
	}
}
