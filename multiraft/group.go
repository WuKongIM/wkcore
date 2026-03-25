package multiraft

import (
	"context"
	"math"
	"sync"

	raft "go.etcd.io/raft/v3"
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
	rawNode      *raft.RawNode
	requests     []raftpb.Message
	requestCount int
	controls     []controlAction
	proposals    []*future
	configs      []*future
	tickPending  bool
	tickCount    int
}

type controlKind uint8

const (
	controlPropose controlKind = iota + 1
	controlCampaign
	controlConfigChange
	controlTransferLeader
)

type controlAction struct {
	kind   controlKind
	data   []byte
	future *future
	target NodeID
	change ConfigChange
}

func newGroup(ctx context.Context, nodeID NodeID, raftOpts RaftOptions, opts GroupOptions) (*group, error) {
	state, snapshot, memory, err := newStorageAdapter(opts.Storage).load(ctx)
	if err != nil {
		return nil, err
	}

	rawNode, err := raft.NewRawNode(&raft.Config{
		ID:              uint64(nodeID),
		ElectionTick:    raftOpts.ElectionTick,
		HeartbeatTick:   raftOpts.HeartbeatTick,
		Storage:         memory,
		Applied:         state.AppliedIndex,
		MaxSizePerMsg:   maxSizePerMsg(raftOpts.MaxSizePerMsg),
		MaxInflightMsgs: maxInflight(raftOpts.MaxInflight),
		CheckQuorum:     raftOpts.CheckQuorum,
		PreVote:         raftOpts.PreVote,
	})
	if err != nil {
		return nil, err
	}

	g := &group{
		id:           opts.ID,
		storage:      opts.Storage,
		stateMachine: opts.StateMachine,
		status: Status{
			GroupID:      opts.ID,
			NodeID:       nodeID,
			LeaderID:     NodeID(state.HardState.Vote),
			CommitIndex:  state.HardState.Commit,
			AppliedIndex: state.AppliedIndex,
		},
		storageView: newStorageAdapter(opts.Storage),
		rawNode:     rawNode,
	}
	g.storageView.memory = memory
	if !raft.IsEmptySnap(snapshot) {
		if err := g.stateMachine.Restore(ctx, Snapshot{
			Index: snapshot.Metadata.Index,
			Term:  snapshot.Metadata.Term,
			Data:  append([]byte(nil), snapshot.Data...),
		}); err != nil {
			return nil, err
		}
	}
	g.refreshStatus()
	return g, nil
}

func (g *group) enqueueRequest(msg raftpb.Message) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.requests = append(g.requests, msg)
}

func (g *group) processRequests() {
	g.mu.Lock()
	requests := append([]raftpb.Message(nil), g.requests...)
	g.requestCount += len(g.requests)
	g.requests = g.requests[:0]
	g.mu.Unlock()

	for _, msg := range requests {
		_ = g.rawNode.Step(msg)
	}
}

func (g *group) enqueueControl(action controlAction) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.controls = append(g.controls, action)
}

func (g *group) processControls() {
	g.mu.Lock()
	controls := append([]controlAction(nil), g.controls...)
	g.controls = g.controls[:0]
	g.mu.Unlock()

	for _, action := range controls {
		switch action.kind {
		case controlPropose:
			if err := g.rawNode.Propose(action.data); err != nil {
				action.future.resolve(Result{}, err)
				continue
			}
			g.mu.Lock()
			g.proposals = append(g.proposals, action.future)
			g.mu.Unlock()
		case controlConfigChange:
			cc, err := toRaftConfChange(action.change)
			if err != nil {
				action.future.resolve(Result{}, err)
				continue
			}
			if err := g.rawNode.ProposeConfChange(cc); err != nil {
				action.future.resolve(Result{}, err)
				continue
			}
			g.mu.Lock()
			g.configs = append(g.configs, action.future)
			g.mu.Unlock()
		case controlCampaign:
			_ = g.rawNode.Campaign()
		case controlTransferLeader:
			g.rawNode.TransferLeader(uint64(action.target))
		}
	}
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
	g.rawNode.Tick()
}

func (g *group) processReady(ctx context.Context, transport Transport) bool {
	if !g.rawNode.HasReady() {
		return false
	}

	ready := g.rawNode.Ready()
	if err := g.storageView.persistReady(ctx, ready); err != nil {
		g.failPending(err)
		g.rawNode.Advance(ready)
		return g.rawNode.HasReady()
	}

	if len(ready.Messages) > 0 {
		_ = transport.Send(ctx, wrapMessages(g.id, ready.Messages))
	}

	lastApplied := g.status.AppliedIndex
	if !raft.IsEmptySnap(ready.Snapshot) {
		_ = g.stateMachine.Restore(ctx, Snapshot{
			Index: ready.Snapshot.Metadata.Index,
			Term:  ready.Snapshot.Metadata.Term,
			Data:  append([]byte(nil), ready.Snapshot.Data...),
		})
		lastApplied = ready.Snapshot.Metadata.Index
	}

	for _, entry := range ready.CommittedEntries {
		lastApplied = entry.Index
		switch entry.Type {
		case raftpb.EntryNormal:
			if len(entry.Data) == 0 {
				continue
			}
			result, err := g.stateMachine.Apply(ctx, Command{
				GroupID: g.id,
				Index:   entry.Index,
				Term:    entry.Term,
				Data:    append([]byte(nil), entry.Data...),
			})
			g.resolveNextProposal(Result{
				Index: entry.Index,
				Term:  entry.Term,
				Data:  result,
			}, err)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				g.resolveNextConfig(Result{}, err)
				continue
			}
			g.rawNode.ApplyConfChange(cc)
			g.resolveNextConfig(Result{
				Index: entry.Index,
				Term:  entry.Term,
			}, nil)
		}
	}

	if lastApplied > 0 {
		_ = g.storage.MarkApplied(ctx, lastApplied)
	}

	g.rawNode.Advance(ready)
	return g.rawNode.HasReady()
}

func (g *group) refreshStatus() {
	st := g.rawNode.Status()
	g.mu.Lock()
	defer g.mu.Unlock()
	g.status.LeaderID = NodeID(st.Lead)
	g.status.Term = st.Term
	g.status.CommitIndex = st.Commit
	g.status.AppliedIndex = st.Applied
	g.status.Role = mapRole(st.RaftState)
}

func (g *group) resolveNextProposal(result Result, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.proposals) == 0 {
		return
	}
	fut := g.proposals[0]
	g.proposals = g.proposals[1:]
	fut.resolve(result, err)
}

func (g *group) failPending(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.failPendingLocked(err)
}

func (g *group) failPendingLocked(err error) {
	for _, fut := range g.proposals {
		fut.resolve(Result{}, err)
	}
	for _, fut := range g.configs {
		fut.resolve(Result{}, err)
	}
	g.proposals = nil
	g.configs = nil
}

func (g *group) resolveNextConfig(result Result, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.configs) == 0 {
		return
	}
	fut := g.configs[0]
	g.configs = g.configs[1:]
	fut.resolve(result, err)
}

func wrapMessages(groupID GroupID, messages []raftpb.Message) []Envelope {
	out := make([]Envelope, 0, len(messages))
	for _, msg := range messages {
		out = append(out, Envelope{
			GroupID: groupID,
			Message: msg,
		})
	}
	return out
}

func mapRole(state raft.StateType) Role {
	switch state {
	case raft.StateLeader:
		return RoleLeader
	case raft.StateCandidate:
		return RoleCandidate
	default:
		return RoleFollower
	}
}

func maxSizePerMsg(v uint64) uint64 {
	if v == 0 {
		return math.MaxUint64
	}
	return v
}

func maxInflight(v int) int {
	if v <= 0 {
		return 256
	}
	return v
}

func toRaftConfChange(change ConfigChange) (raftpb.ConfChange, error) {
	cc := raftpb.ConfChange{
		NodeID:  uint64(change.NodeID),
		Context: append([]byte(nil), change.Context...),
	}

	switch change.Type {
	case AddVoter:
		cc.Type = raftpb.ConfChangeAddNode
	case RemoveVoter:
		cc.Type = raftpb.ConfChangeRemoveNode
	case AddLearner:
		cc.Type = raftpb.ConfChangeAddLearnerNode
	case PromoteLearner:
		cc.Type = raftpb.ConfChangeAddNode
	default:
		return raftpb.ConfChange{}, errNotImplemented
	}
	return cc, nil
}
