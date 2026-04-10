package isr

import "context"

func (r *replica) Fetch(_ context.Context, req FetchRequest) (FetchResult, error) {
	r.mu.Lock()

	if r.state.Role == RoleTombstoned {
		r.mu.Unlock()
		return FetchResult{}, ErrTombstoned
	}
	if req.MaxBytes <= 0 {
		r.mu.Unlock()
		return FetchResult{}, ErrInvalidFetchBudget
	}
	if req.ReplicaID == 0 {
		r.mu.Unlock()
		return FetchResult{}, ErrInvalidMeta
	}
	if r.state.Role != RoleLeader && r.state.Role != RoleFencedLeader {
		r.mu.Unlock()
		return FetchResult{}, ErrNotLeader
	}
	if r.state.GroupKey != "" && req.GroupKey != r.state.GroupKey {
		r.mu.Unlock()
		return FetchResult{}, ErrStaleMeta
	}
	if req.Epoch != r.state.Epoch {
		r.mu.Unlock()
		return FetchResult{}, ErrStaleMeta
	}
	if req.FetchOffset < r.state.LogStartOffset {
		r.mu.Unlock()
		return FetchResult{}, ErrSnapshotRequired
	}

	leaderLEO := r.state.LEO
	r.state.OffsetEpoch = offsetEpochForLEO(r.epochHistory, leaderLEO)
	needsAdvance := r.progress[r.localNode] != leaderLEO
	r.setReplicaProgressLocked(r.localNode, leaderLEO)

	matchOffset, truncateTo := r.divergenceStateLocked(req.FetchOffset, req.OffsetEpoch, leaderLEO)
	if r.progress[req.ReplicaID] != matchOffset {
		needsAdvance = true
	}
	r.setReplicaProgressLocked(req.ReplicaID, matchOffset)
	result := FetchResult{
		Epoch: r.state.Epoch,
		HW:    r.state.HW,
	}
	r.mu.Unlock()

	if needsAdvance {
		if err := r.advanceHW(); err != nil {
			return FetchResult{}, err
		}
		result.HW = r.Status().HW
	}
	if truncateTo != nil {
		result.TruncateTo = truncateTo
		return result, nil
	}
	if req.FetchOffset >= leaderLEO {
		return result, nil
	}

	records, err := r.log.Read(req.FetchOffset, req.MaxBytes)
	if err != nil {
		return FetchResult{}, err
	}
	result.Records = records
	return result, nil
}
