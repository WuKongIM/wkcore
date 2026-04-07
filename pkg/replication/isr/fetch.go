package isr

import "context"

func (r *replica) Fetch(_ context.Context, req FetchRequest) (FetchResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state.Role == RoleTombstoned {
		return FetchResult{}, ErrTombstoned
	}
	if req.MaxBytes <= 0 {
		return FetchResult{}, ErrInvalidFetchBudget
	}
	if req.ReplicaID == 0 {
		return FetchResult{}, ErrInvalidMeta
	}
	if r.state.Role != RoleLeader && r.state.Role != RoleFencedLeader {
		return FetchResult{}, ErrNotLeader
	}
	if r.state.GroupKey != "" && req.GroupKey != r.state.GroupKey {
		return FetchResult{}, ErrStaleMeta
	}
	if req.Epoch != r.state.Epoch {
		return FetchResult{}, ErrStaleMeta
	}
	if req.FetchOffset < r.state.LogStartOffset {
		return FetchResult{}, ErrSnapshotRequired
	}

	leaderLEO := r.log.LEO()
	r.state.LEO = leaderLEO
	r.state.OffsetEpoch = offsetEpochForLEO(r.epochHistory, leaderLEO)
	r.setReplicaProgressLocked(r.localNode, leaderLEO)

	matchOffset, truncateTo := r.divergenceStateLocked(req.FetchOffset, req.OffsetEpoch, leaderLEO)
	r.setReplicaProgressLocked(req.ReplicaID, matchOffset)
	if err := r.advanceHWLocked(); err != nil {
		return FetchResult{}, err
	}
	if truncateTo != nil {
		return FetchResult{
			Epoch:      r.state.Epoch,
			HW:         r.state.HW,
			TruncateTo: truncateTo,
		}, nil
	}

	records, err := r.log.Read(req.FetchOffset, req.MaxBytes)
	if err != nil {
		return FetchResult{}, err
	}
	return FetchResult{
		Epoch:   r.state.Epoch,
		HW:      r.state.HW,
		Records: records,
	}, nil
}
