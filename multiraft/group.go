package multiraft

import (
	"context"
	"sync"

	"go.etcd.io/raft/v3/raftpb"
)

type group struct {
	mu           sync.Mutex
	id           GroupID
	storage      Storage
	stateMachine StateMachine
	status       Status
	storageView  *storageAdapter
	closed       bool
	requests     []raftpb.Message
	requestCount int
	tickPending  bool
	tickCount    int
}

func newGroup(ctx context.Context, nodeID NodeID, opts GroupOptions) (*group, error) {
	state, err := opts.Storage.InitialState(ctx)
	if err != nil {
		return nil, err
	}

	return &group{
		id:           opts.ID,
		storage:      opts.Storage,
		stateMachine: opts.StateMachine,
		status: Status{
			GroupID:      opts.ID,
			NodeID:       nodeID,
			CommitIndex:  state.HardState.Commit,
			AppliedIndex: state.AppliedIndex,
		},
		storageView: newStorageAdapter(opts.Storage),
	}, nil
}

func (g *group) enqueueRequest(msg raftpb.Message) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.requests = append(g.requests, msg)
}

func (g *group) processRequests() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.requests) == 0 {
		return
	}
	g.requestCount += len(g.requests)
	g.requests = g.requests[:0]
}

func (g *group) markTickPending() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.tickPending = true
}

func (g *group) processTick() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.tickPending {
		return
	}
	g.tickPending = false
	g.tickCount++
}
