package multiraft

import (
	"context"
	"sync"
	"time"
)

type Runtime struct {
	opts Options

	mu        sync.RWMutex
	closed    bool
	groups    map[GroupID]*group
	scheduler *scheduler
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

func New(opts Options) (*Runtime, error) {
	if opts.NodeID == 0 ||
		opts.TickInterval <= 0 ||
		opts.Workers <= 0 ||
		opts.Transport == nil ||
		opts.Raft.ElectionTick <= 0 ||
		opts.Raft.HeartbeatTick <= 0 ||
		opts.Raft.ElectionTick <= opts.Raft.HeartbeatTick {
		return nil, ErrInvalidOptions
	}

	rt := &Runtime{
		opts:      opts,
		groups:    make(map[GroupID]*group),
		scheduler: newScheduler(),
		stopCh:    make(chan struct{}),
	}
	rt.start()
	return rt, nil
}

func (r *Runtime) start() {
	for i := 0; i < r.opts.Workers; i++ {
		r.wg.Add(1)
		go r.runWorker()
	}

	r.wg.Add(1)
	go r.runTicker()
}

func (r *Runtime) runWorker() {
	defer r.wg.Done()

	for {
		select {
		case <-r.stopCh:
			return
		case groupID := <-r.scheduler.ch:
			r.scheduler.begin(groupID)
			requeue := r.processGroup(groupID)
			if r.scheduler.done(groupID) || requeue {
				r.scheduler.requeue(groupID)
			}
		}
	}
}

func (r *Runtime) runTicker() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.opts.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.mu.RLock()
			groups := make([]*group, 0, len(r.groups))
			for _, g := range r.groups {
				groups = append(groups, g)
			}
			r.mu.RUnlock()
			for _, g := range groups {
				g.markTickPending()
				r.scheduler.enqueue(g.id)
			}
		}
	}
}

func (r *Runtime) processGroup(groupID GroupID) bool {
	r.mu.RLock()
	g := r.groups[groupID]
	r.mu.RUnlock()
	if g == nil {
		return false
	}
	if !g.beginProcessing() {
		return false
	}
	defer g.finishProcessing()

	g.processRequests()
	if !g.shouldProcess() {
		return false
	}
	g.processControls()
	if !g.shouldProcess() {
		return false
	}
	g.processTick()
	if !g.shouldProcess() {
		return false
	}
	if g.processReady(context.Background(), r.opts.Transport) {
		g.refreshStatus()
		return true
	}
	g.refreshStatus()
	return false
}
