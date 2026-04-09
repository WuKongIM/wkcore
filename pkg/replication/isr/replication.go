package isr

import "context"

func (r *replica) ApplyFetch(_ context.Context, req ApplyFetchRequest) error {
	r.appendMu.Lock()
	defer r.appendMu.Unlock()

	r.mu.Lock()

	if r.state.Role == RoleTombstoned {
		r.mu.Unlock()
		return ErrTombstoned
	}
	if r.state.Role != RoleFollower && r.state.Role != RoleFencedLeader {
		r.mu.Unlock()
		return ErrNotLeader
	}
	if r.state.GroupKey != "" && req.GroupKey != r.state.GroupKey {
		r.mu.Unlock()
		return ErrStaleMeta
	}
	if req.Epoch != r.state.Epoch {
		r.mu.Unlock()
		return ErrStaleMeta
	}
	if r.state.Leader != 0 && req.Leader != r.state.Leader {
		r.mu.Unlock()
		return ErrStaleMeta
	}

	leo := r.log.LEO()
	if req.TruncateTo != nil {
		if *req.TruncateTo < r.state.HW || *req.TruncateTo > leo {
			r.mu.Unlock()
			return ErrCorruptState
		}
		if err := r.truncateLogToLocked(*req.TruncateTo); err != nil {
			r.mu.Unlock()
			return err
		}
		leo = *req.TruncateTo
	}

	if len(req.Records) > 0 {
		if len(r.epochHistory) == 0 || r.epochHistory[len(r.epochHistory)-1].Epoch != req.Epoch {
			if err := r.appendEpochPointLocked(EpochPoint{Epoch: req.Epoch, StartOffset: leo}); err != nil {
				r.mu.Unlock()
				return err
			}
		}
		if req.TruncateTo == nil && r.applyFetch != nil {
			nextLEO := leo + uint64(len(req.Records))
			nextHW := req.LeaderHW
			if nextHW > nextLEO {
				nextHW = nextLEO
			}
			if nextHW < r.state.HW {
				r.mu.Unlock()
				return ErrCorruptState
			}

			var checkpoint *Checkpoint
			if nextHW > r.state.HW {
				value := Checkpoint{
					Epoch:          req.Epoch,
					LogStartOffset: r.state.LogStartOffset,
					HW:             nextHW,
				}
				checkpoint = &value
			}

			r.mu.Unlock()
			storedLEO, err := r.applyFetch.StoreApplyFetch(ApplyFetchStoreRequest{
				Records:    req.Records,
				Checkpoint: checkpoint,
			})
			if err != nil {
				return err
			}
			if storedLEO != nextLEO {
				return ErrCorruptState
			}

			r.mu.Lock()
			r.state.LEO = storedLEO
			if checkpoint != nil {
				r.state.HW = checkpoint.HW
			}
			r.state.OffsetEpoch = offsetEpochForLEO(r.epochHistory, storedLEO)
			r.mu.Unlock()
			return nil
		}
		if _, err := r.log.Append(req.Records); err != nil {
			r.mu.Unlock()
			return err
		}
		r.mu.Unlock()
		if err := r.log.Sync(); err != nil {
			return err
		}
		r.mu.Lock()
		leo = r.log.LEO()
	}

	nextHW := req.LeaderHW
	if nextHW > leo {
		nextHW = leo
	}
	if nextHW < r.state.HW {
		r.mu.Unlock()
		return ErrCorruptState
	}
	if nextHW == r.state.HW {
		r.state.LEO = leo
		r.state.OffsetEpoch = offsetEpochForLEO(r.epochHistory, leo)
		r.mu.Unlock()
		return nil
	}

	checkpoint := Checkpoint{
		Epoch:          req.Epoch,
		LogStartOffset: r.state.LogStartOffset,
		HW:             nextHW,
	}
	if err := r.checkpoints.Store(checkpoint); err != nil {
		r.mu.Unlock()
		return err
	}

	r.state.LEO = leo
	r.state.HW = nextHW
	r.state.OffsetEpoch = offsetEpochForLEO(r.epochHistory, leo)
	r.mu.Unlock()
	return nil
}
