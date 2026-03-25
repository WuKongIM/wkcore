package multiraft

import "context"

func (r *Runtime) Close() error {
	return nil
}

func (r *Runtime) OpenGroup(ctx context.Context, opts GroupOptions) error {
	return errNotImplemented
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
	return nil
}
