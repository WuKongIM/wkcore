package isr

import "context"

func (r *replica) ApplyProgressAck(_ context.Context, req ProgressAckRequest) error {
	r.mu.Lock()

	if r.state.Role == RoleTombstoned {
		r.mu.Unlock()
		return ErrTombstoned
	}
	if r.state.Role != RoleLeader && r.state.Role != RoleFencedLeader {
		r.mu.Unlock()
		return ErrNotLeader
	}
	if req.ChannelKey != "" && req.ChannelKey != r.state.ChannelKey {
		r.mu.Unlock()
		return ErrStaleMeta
	}
	if req.Epoch != r.state.Epoch {
		r.mu.Unlock()
		return ErrStaleMeta
	}
	if req.ReplicaID == 0 {
		r.mu.Unlock()
		return ErrInvalidMeta
	}
	current := r.progress[req.ReplicaID]
	if req.MatchOffset <= current {
		r.mu.Unlock()
		return nil
	}
	if req.MatchOffset > r.state.LEO {
		r.mu.Unlock()
		return ErrCorruptState
	}
	r.setReplicaProgressLocked(req.ReplicaID, req.MatchOffset)
	r.mu.Unlock()

	return r.advanceHW()
}
