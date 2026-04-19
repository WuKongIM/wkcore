package replica

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

func (r *replica) Fetch(_ context.Context, req channel.ReplicaFetchRequest) (channel.ReplicaFetchResult, error) {
	r.mu.Lock()

	if r.state.Role == channel.ReplicaRoleTombstoned {
		r.mu.Unlock()
		return channel.ReplicaFetchResult{}, channel.ErrTombstoned
	}
	if req.MaxBytes <= 0 {
		r.mu.Unlock()
		return channel.ReplicaFetchResult{}, channel.ErrInvalidFetchBudget
	}
	if req.ReplicaID == 0 {
		r.mu.Unlock()
		return channel.ReplicaFetchResult{}, channel.ErrInvalidMeta
	}
	if r.state.Role != channel.ReplicaRoleLeader && r.state.Role != channel.ReplicaRoleFencedLeader {
		r.mu.Unlock()
		return channel.ReplicaFetchResult{}, channel.ErrNotLeader
	}
	if r.state.ChannelKey != "" && req.ChannelKey != r.state.ChannelKey {
		r.mu.Unlock()
		return channel.ReplicaFetchResult{}, channel.ErrStaleMeta
	}
	if req.Epoch != r.state.Epoch {
		r.mu.Unlock()
		return channel.ReplicaFetchResult{}, channel.ErrStaleMeta
	}
	if req.FetchOffset < r.state.LogStartOffset {
		r.mu.Unlock()
		return channel.ReplicaFetchResult{}, channel.ErrSnapshotRequired
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
	result := channel.ReplicaFetchResult{
		Epoch: r.state.Epoch,
		HW:    visibleCommittedHW(r.state),
	}
	r.publishStateLocked()
	r.mu.Unlock()

	if needsAdvance {
		r.signalAdvanceHW()
		result.HW = visibleCommittedHW(r.Status())
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
		return channel.ReplicaFetchResult{}, err
	}
	result.Records = records
	return result, nil
}

func visibleCommittedHW(state channel.ReplicaState) uint64 {
	if state.CommitReady {
		return state.HW
	}
	if state.CheckpointHW < state.HW {
		return state.CheckpointHW
	}
	return state.HW
}
