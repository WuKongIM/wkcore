package gnet

import (
	"sync"

	"github.com/WuKongIM/WuKongIM/internal/gateway/transport"
	gnetv2 "github.com/panjf2000/gnet/v2"
)

type engineGroup struct {
	mu     sync.Mutex
	engine gnetv2.Engine
	specs  []transport.ListenerSpec
	refs   int
}

func newEngineGroup(specs []transport.ListenerSpec) *engineGroup {
	return &engineGroup{
		specs: append([]transport.ListenerSpec(nil), specs...),
	}
}

func (g *engineGroup) start() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.refs++
	return nil
}

func (g *engineGroup) stop() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.refs > 0 {
		g.refs--
	}
	return nil
}

type listenerHandle struct {
	mu      sync.Mutex
	opts    transport.ListenerOptions
	group   *engineGroup
	started bool
}

func (h *listenerHandle) Start() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.started {
		return nil
	}
	if err := h.group.start(); err != nil {
		return err
	}
	h.started = true
	return nil
}

func (h *listenerHandle) Stop() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.started {
		return nil
	}
	if err := h.group.stop(); err != nil {
		return err
	}
	h.started = false
	return nil
}

func (h *listenerHandle) Addr() string {
	return h.opts.Address
}

var _ transport.Listener = (*listenerHandle)(nil)
