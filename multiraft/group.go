package multiraft

import "context"

type group struct {
	id           GroupID
	storage      Storage
	stateMachine StateMachine
	status       Status
	storageView  *storageAdapter
	closed       bool
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
