package multiraft

import (
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
			r.processGroup(groupID)
			r.scheduler.done(groupID)
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
			for _, g := range r.groups {
				g.markTickPending()
				r.scheduler.enqueue(g.id)
			}
			r.mu.RUnlock()
		}
	}
}

func (r *Runtime) processGroup(groupID GroupID) {
	r.mu.RLock()
	g := r.groups[groupID]
	r.mu.RUnlock()
	if g == nil {
		return
	}

	g.processRequests()
	g.processTick()
}
