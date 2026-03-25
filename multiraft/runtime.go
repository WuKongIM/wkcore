package multiraft

import "sync"

type Runtime struct {
	opts Options

	mu     sync.RWMutex
	groups map[GroupID]*group
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

	return &Runtime{
		opts:   opts,
		groups: make(map[GroupID]*group),
	}, nil
}
