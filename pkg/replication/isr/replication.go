package isr

import "context"

func (r *replica) ApplyFetch(_ context.Context, req ApplyFetchRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state.Role == RoleTombstoned {
		return ErrTombstoned
	}
	if r.state.Role != RoleFollower && r.state.Role != RoleFencedLeader {
		return ErrNotLeader
	}
	if r.state.GroupKey != "" && req.GroupKey != r.state.GroupKey {
		return ErrStaleMeta
	}
	if req.Epoch != r.state.Epoch {
		return ErrStaleMeta
	}
	if r.state.Leader != 0 && req.Leader != r.state.Leader {
		return ErrStaleMeta
	}

	leo := r.log.LEO()
	if req.TruncateTo != nil {
		if *req.TruncateTo < r.state.HW || *req.TruncateTo > leo {
			return ErrCorruptState
		}
		if err := r.log.Truncate(*req.TruncateTo); err != nil {
			return err
		}
		if err := r.log.Sync(); err != nil {
			return err
		}
		leo = r.log.LEO()
		if leo != *req.TruncateTo {
			return ErrCorruptState
		}
	}

	if len(req.Records) > 0 {
		if _, err := r.log.Append(req.Records); err != nil {
			return err
		}
		if err := r.log.Sync(); err != nil {
			return err
		}
		leo = r.log.LEO()
	}

	nextHW := req.LeaderHW
	if nextHW > leo {
		nextHW = leo
	}
	if nextHW < r.state.HW {
		return ErrCorruptState
	}

	checkpoint := Checkpoint{
		Epoch:          req.Epoch,
		LogStartOffset: r.state.LogStartOffset,
		HW:             nextHW,
	}
	if err := r.checkpoints.Store(checkpoint); err != nil {
		return err
	}

	r.state.LEO = leo
	r.state.HW = nextHW
	return nil
}
