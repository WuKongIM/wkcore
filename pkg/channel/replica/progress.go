package replica

import (
	"context"
	"slices"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

const smallISRProgressBufferSize = 8

func (r *replica) seedLeaderProgressLocked(isr []channel.NodeID, leaderLEO, committedHW uint64) {
	r.progress = make(map[channel.NodeID]uint64, len(isr))
	for _, id := range isr {
		if id == r.localNode {
			r.progress[id] = leaderLEO
			continue
		}
		r.progress[id] = committedHW
	}
}

func (r *replica) setReplicaProgressLocked(replicaID channel.NodeID, matchOffset uint64) {
	if r.progress == nil {
		r.progress = make(map[channel.NodeID]uint64)
	}
	r.progress[replicaID] = matchOffset
}

func (r *replica) advanceHW() error {
	r.advanceMu.Lock()
	defer r.advanceMu.Unlock()

	r.mu.Lock()
	checkpoint, candidate, err := r.nextHWCheckpointLocked()
	r.mu.Unlock()
	if err != nil || checkpoint == nil {
		return err
	}

	if err := r.checkpoints.Store(*checkpoint); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if candidate <= r.state.HW {
		return nil
	}
	if candidate > r.state.LEO {
		return channel.ErrCorruptState
	}

	r.state.HW = candidate
	r.notifyReadyWaitersLocked()
	r.publishStateLocked()
	return nil
}

func (r *replica) nextHWCheckpointLocked() (*channel.Checkpoint, uint64, error) {
	if r.state.Role != channel.ReplicaRoleLeader && r.state.Role != channel.ReplicaRoleFencedLeader {
		return nil, 0, nil
	}
	if len(r.meta.ISR) == 0 || r.meta.MinISR == 0 {
		return nil, 0, nil
	}

	var smallMatches [smallISRProgressBufferSize]uint64
	matches := smallMatches[:0]
	if len(r.meta.ISR) > smallISRProgressBufferSize {
		matches = make([]uint64, 0, len(r.meta.ISR))
	}
	for _, id := range r.meta.ISR {
		matches = append(matches, r.progress[id])
	}
	if len(matches) < r.meta.MinISR {
		return nil, 0, nil
	}

	slices.Sort(matches)
	candidate := matches[len(matches)-r.meta.MinISR]
	if candidate <= r.state.HW {
		return nil, 0, nil
	}
	if candidate > r.state.LEO {
		return nil, 0, channel.ErrCorruptState
	}

	checkpoint := channel.Checkpoint{
		Epoch:          r.state.Epoch,
		LogStartOffset: r.state.LogStartOffset,
		HW:             candidate,
	}
	return &checkpoint, candidate, nil
}

func (r *replica) notifyReadyWaitersLocked() {
	if len(r.waiters) == 0 {
		return
	}

	remaining := r.waiters[:0]
	for _, waiter := range r.waiters {
		if r.state.HW >= waiter.target {
			waiter.result.NextCommitHW = r.state.HW
			r.completeAppendWaiter(waiter, waiter.result, nil)
			continue
		}
		remaining = append(remaining, waiter)
	}
	r.waiters = remaining
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

func (r *replica) ApplyProgressAck(_ context.Context, req channel.ReplicaProgressAckRequest) error {
	r.mu.Lock()

	if r.state.Role == channel.ReplicaRoleTombstoned {
		r.mu.Unlock()
		return channel.ErrTombstoned
	}
	if r.state.Role != channel.ReplicaRoleLeader && r.state.Role != channel.ReplicaRoleFencedLeader {
		r.mu.Unlock()
		return channel.ErrNotLeader
	}
	if req.ChannelKey != "" && req.ChannelKey != r.state.ChannelKey {
		r.mu.Unlock()
		return channel.ErrStaleMeta
	}
	if req.Epoch != r.state.Epoch {
		r.mu.Unlock()
		return channel.ErrStaleMeta
	}
	if req.ReplicaID == 0 {
		r.mu.Unlock()
		return channel.ErrInvalidMeta
	}
	current := r.progress[req.ReplicaID]
	if req.MatchOffset <= current {
		r.mu.Unlock()
		return nil
	}
	if req.MatchOffset > r.state.LEO {
		r.mu.Unlock()
		return channel.ErrCorruptState
	}
	r.setReplicaProgressLocked(req.ReplicaID, req.MatchOffset)
	r.publishStateLocked()
	r.mu.Unlock()

	return r.advanceHW()
}
