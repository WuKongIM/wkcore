package multiraft

import (
	"context"
	"sort"
)

func (r *Runtime) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	close(r.stopCh)
	r.mu.Unlock()

	r.wg.Wait()

	r.mu.Lock()
	r.groups = make(map[GroupID]*group)
	r.mu.Unlock()
	return nil
}

func (r *Runtime) OpenGroup(ctx context.Context, opts GroupOptions) error {
	if err := validateGroupOptions(opts); err != nil {
		return err
	}

	g, err := newGroup(ctx, r.opts.NodeID, opts)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRuntimeClosed
	}
	if _, exists := r.groups[opts.ID]; exists {
		return ErrGroupExists
	}
	r.groups[opts.ID] = g
	return nil
}

func (r *Runtime) BootstrapGroup(ctx context.Context, req BootstrapGroupRequest) error {
	if err := validateGroupOptions(req.Group); err != nil {
		return err
	}

	g, err := newGroup(ctx, r.opts.NodeID, req.Group)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRuntimeClosed
	}
	if _, exists := r.groups[req.Group.ID]; exists {
		return ErrGroupExists
	}
	r.groups[req.Group.ID] = g
	return nil
}

func (r *Runtime) CloseGroup(ctx context.Context, groupID GroupID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrRuntimeClosed
	}
	g, ok := r.groups[groupID]
	if !ok {
		return ErrGroupNotFound
	}
	g.closed = true
	delete(r.groups, groupID)
	return nil
}

func (r *Runtime) Step(ctx context.Context, msg Envelope) error {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return ErrRuntimeClosed
	}
	g, ok := r.groups[msg.GroupID]
	r.mu.RUnlock()
	if !ok {
		return ErrGroupNotFound
	}

	g.enqueueRequest(msg.Message)
	r.scheduler.enqueue(msg.GroupID)
	return nil
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
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return Status{}, ErrRuntimeClosed
	}
	g, ok := r.groups[groupID]
	if !ok || g.closed {
		return Status{}, ErrGroupNotFound
	}
	return g.status, nil
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

func validateGroupOptions(opts GroupOptions) error {
	if opts.ID == 0 || opts.Storage == nil || opts.StateMachine == nil {
		return ErrInvalidOptions
	}
	return nil
}
