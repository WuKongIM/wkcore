package multiraft

import (
	"context"
	"sort"
)

func (r *Runtime) Close() error {
	return nil
}

func (r *Runtime) OpenGroup(ctx context.Context, opts GroupOptions) error {
	g, err := newGroup(ctx, r.opts.NodeID, opts)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.groups[opts.ID]; exists {
		return ErrGroupExists
	}
	r.groups[opts.ID] = g
	return nil
}

func (r *Runtime) BootstrapGroup(ctx context.Context, req BootstrapGroupRequest) error {
	return errNotImplemented
}

func (r *Runtime) CloseGroup(ctx context.Context, groupID GroupID) error {
	return errNotImplemented
}

func (r *Runtime) Step(ctx context.Context, msg Envelope) error {
	return errNotImplemented
}

func (r *Runtime) Propose(ctx context.Context, groupID GroupID, data []byte) (Future, error) {
	return nil, errNotImplemented
}

func (r *Runtime) ChangeConfig(ctx context.Context, groupID GroupID, change ConfigChange) (Future, error) {
	return nil, errNotImplemented
}

func (r *Runtime) TransferLeadership(ctx context.Context, groupID GroupID, target NodeID) error {
	return errNotImplemented
}

func (r *Runtime) Status(groupID GroupID) (Status, error) {
	return Status{}, errNotImplemented
}

func (r *Runtime) Groups() []GroupID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]GroupID, 0, len(r.groups))
	for id := range r.groups {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
