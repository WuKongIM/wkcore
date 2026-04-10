package log

import "slices"

func (c *cluster) ApplyMeta(meta ChannelMeta) error {
	key := metaKey(meta)
	next := cloneMeta(meta)

	c.mu.Lock()
	defer c.mu.Unlock()

	local, ok := c.metas[key]
	if !ok {
		c.metas[key] = next
		return nil
	}

	switch {
	case next.ChannelEpoch < local.ChannelEpoch:
		return ErrStaleMeta
	case next.ChannelEpoch == local.ChannelEpoch && next.LeaderEpoch < local.LeaderEpoch:
		return ErrStaleMeta
	case next.ChannelEpoch == local.ChannelEpoch && next.LeaderEpoch == local.LeaderEpoch:
		if metaEqual(local, next) {
			return nil
		}
		return ErrConflictingMeta
	case next.ChannelEpoch > local.ChannelEpoch:
		c.metas[key] = next
		return nil
	case next.ChannelEpoch == local.ChannelEpoch && next.LeaderEpoch > local.LeaderEpoch:
		c.metas[key] = next
		return nil
	default:
		return ErrStaleMeta
	}
}

func (c *cluster) RemoveMeta(key ChannelKey) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.metas, key)
	return nil
}

func (c *cluster) Status(key ChannelKey) (ChannelRuntimeStatus, error) {
	c.mu.RLock()
	meta, ok := c.metas[key]
	c.mu.RUnlock()
	if !ok {
		return ChannelRuntimeStatus{}, ErrStaleMeta
	}

	group, ok := c.cfg.Runtime.Channel(isrChannelKeyForChannel(key))
	if !ok {
		return ChannelRuntimeStatus{}, ErrStaleMeta
	}
	state := group.Status()
	return ChannelRuntimeStatus{
		Key:          key,
		Status:       meta.Status,
		Leader:       meta.Leader,
		LeaderEpoch:  meta.LeaderEpoch,
		HW:           state.HW,
		CommittedSeq: state.HW,
	}, nil
}

func compatibleWithExpectation(meta ChannelMeta, expectedChannelEpoch, expectedLeaderEpoch uint64) error {
	if expectedChannelEpoch == 0 && expectedLeaderEpoch == 0 {
		return nil
	}
	if expectedChannelEpoch != 0 && meta.ChannelEpoch != expectedChannelEpoch {
		return ErrStaleMeta
	}
	if expectedLeaderEpoch != 0 && meta.LeaderEpoch != expectedLeaderEpoch {
		return ErrStaleMeta
	}
	return nil
}

func metaKey(meta ChannelMeta) ChannelKey {
	return ChannelKey{
		ChannelID:   meta.ChannelID,
		ChannelType: meta.ChannelType,
	}
}

func cloneMeta(meta ChannelMeta) ChannelMeta {
	meta.Replicas = slices.Clone(meta.Replicas)
	meta.ISR = slices.Clone(meta.ISR)
	return meta
}

func metaEqual(a, b ChannelMeta) bool {
	return a.ChannelID == b.ChannelID &&
		a.ChannelType == b.ChannelType &&
		a.ChannelEpoch == b.ChannelEpoch &&
		a.LeaderEpoch == b.LeaderEpoch &&
		slices.Equal(a.Replicas, b.Replicas) &&
		slices.Equal(a.ISR, b.ISR) &&
		a.Leader == b.Leader &&
		a.MinISR == b.MinISR &&
		a.Status == b.Status &&
		a.Features == b.Features
}
