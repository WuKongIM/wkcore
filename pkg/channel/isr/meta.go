package isr

func (r *replica) applyMetaLocked(meta ChannelMeta) error {
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

func (r *replica) validateMetaLocked(normalized ChannelMeta) error {
	switch {
	case r.state.ChannelKey != "" && normalized.ChannelKey != r.state.ChannelKey:
		return ErrInvalidMeta
	case normalized.Epoch < r.state.Epoch:
		return ErrStaleMeta
	case normalized.Epoch == r.state.Epoch && r.state.Leader != 0 && normalized.Leader != r.state.Leader:
		return ErrStaleMeta
	}
	return nil
}

func (r *replica) commitMetaLocked(normalized ChannelMeta) {
	r.meta = normalized
	r.state.ChannelKey = normalized.ChannelKey
	r.state.Epoch = normalized.Epoch
	r.state.Leader = normalized.Leader
}

func normalizeMeta(meta ChannelMeta) (ChannelMeta, error) {
	meta.Replicas = dedupeNodeIDs(meta.Replicas)
	meta.ISR = dedupeNodeIDs(meta.ISR)

	if meta.ChannelKey == "" {
		return ChannelMeta{}, ErrInvalidMeta
	}
	if len(meta.Replicas) == 0 {
		return ChannelMeta{}, ErrInvalidMeta
	}
	if meta.Leader == 0 {
		return ChannelMeta{}, ErrInvalidMeta
	}
	if meta.MinISR < 1 || meta.MinISR > len(meta.Replicas) {
		return ChannelMeta{}, ErrInvalidMeta
	}
	if !containsNode(meta.Replicas, meta.Leader) {
		return ChannelMeta{}, ErrInvalidMeta
	}
	if !containsNode(meta.ISR, meta.Leader) {
		return ChannelMeta{}, ErrInvalidMeta
	}
	for _, id := range meta.ISR {
		if !containsNode(meta.Replicas, id) {
			return ChannelMeta{}, ErrInvalidMeta
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
