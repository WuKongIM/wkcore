package isr

func (r *replica) applyMetaLocked(meta GroupMeta) error {
	normalized, err := normalizeMeta(meta)
	if err != nil {
		return err
	}
	if err := r.validateMetaLocked(normalized); err != nil {
		return err
	}
	r.commitMetaLocked(normalized)
	return nil
}

func (r *replica) validateMetaLocked(normalized GroupMeta) error {
	switch {
	case r.state.GroupID != 0 && normalized.GroupID != r.state.GroupID:
		return ErrInvalidMeta
	case normalized.Epoch < r.state.Epoch:
		return ErrStaleMeta
	case normalized.Epoch == r.state.Epoch && r.state.GroupID != 0 && normalized.GroupID != r.state.GroupID:
		return ErrStaleMeta
	case normalized.Epoch == r.state.Epoch && r.state.Leader != 0 && normalized.Leader != r.state.Leader:
		return ErrStaleMeta
	}
	return nil
}

func (r *replica) commitMetaLocked(normalized GroupMeta) {
	r.meta = normalized
	r.state.GroupID = normalized.GroupID
	r.state.Epoch = normalized.Epoch
	r.state.Leader = normalized.Leader
}

func normalizeMeta(meta GroupMeta) (GroupMeta, error) {
	meta.Replicas = dedupeNodeIDs(meta.Replicas)
	meta.ISR = dedupeNodeIDs(meta.ISR)

	if meta.GroupID == 0 {
		return GroupMeta{}, ErrInvalidMeta
	}
	if len(meta.Replicas) == 0 {
		return GroupMeta{}, ErrInvalidMeta
	}
	if meta.Leader == 0 {
		return GroupMeta{}, ErrInvalidMeta
	}
	if meta.MinISR < 1 || meta.MinISR > len(meta.Replicas) {
		return GroupMeta{}, ErrInvalidMeta
	}
	if !containsNode(meta.Replicas, meta.Leader) {
		return GroupMeta{}, ErrInvalidMeta
	}
	if !containsNode(meta.ISR, meta.Leader) {
		return GroupMeta{}, ErrInvalidMeta
	}
	for _, id := range meta.ISR {
		if !containsNode(meta.Replicas, id) {
			return GroupMeta{}, ErrInvalidMeta
		}
	}

	return meta, nil
}

func dedupeNodeIDs(ids []NodeID) []NodeID {
	if len(ids) == 0 {
		return nil
	}

	out := make([]NodeID, 0, len(ids))
	seen := make(map[NodeID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func containsNode(ids []NodeID, target NodeID) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}
