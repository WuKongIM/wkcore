package isr

func (r *replica) seedLeaderProgressLocked(isr []NodeID, leaderLEO, committedHW uint64) {
	r.progress = make(map[NodeID]uint64, len(isr))
	for _, id := range isr {
		if id == r.localNode {
			r.progress[id] = leaderLEO
			continue
		}
		r.progress[id] = committedHW
	}
}

func (r *replica) setReplicaProgressLocked(replicaID NodeID, matchOffset uint64) {
	if r.progress == nil {
		r.progress = make(map[NodeID]uint64)
	}
	r.progress[replicaID] = matchOffset
}

func (r *replica) divergenceStateLocked(fetchOffset, offsetEpoch, leaderLEO uint64) (uint64, *uint64) {
	if len(r.epochHistory) == 0 || offsetEpoch == 0 {
		return fetchOffset, nil
	}

	matchOffset := uint64(0)
	index := -1
	for i, point := range r.epochHistory {
		if point.Epoch <= offsetEpoch {
			index = i
			continue
		}
		break
	}
	if index >= 0 {
		matchOffset = leaderLEO
		if index+1 < len(r.epochHistory) {
			matchOffset = r.epochHistory[index+1].StartOffset
		}
	}
	if fetchOffset > matchOffset {
		truncateTo := matchOffset
		return matchOffset, &truncateTo
	}
	return fetchOffset, nil
}
