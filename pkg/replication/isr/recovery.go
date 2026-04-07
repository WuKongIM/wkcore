package isr

import (
	"context"
	"errors"
)

func (r *replica) recoverFromStores() error {
	checkpoint, err := r.checkpoints.Load()
	if errors.Is(err, ErrEmptyState) {
		checkpoint = Checkpoint{}
	} else if err != nil {
		return err
	}

	history, err := r.history.Load()
	if errors.Is(err, ErrEmptyState) {
		history = nil
	} else if err != nil {
		return err
	}

	if err := validateCheckpoint(checkpoint); err != nil {
		return err
	}
	if err := validateEpochHistory(history); err != nil {
		return err
	}

	leo := r.log.LEO()
	if checkpoint.HW > leo {
		return ErrCorruptState
	}
	if len(history) > 0 && history[len(history)-1].StartOffset > leo {
		return ErrCorruptState
	}

	if leo > checkpoint.HW {
		if err := r.truncateLogToLocked(checkpoint.HW); err != nil {
			return err
		}
		leo = checkpoint.HW
		history = trimEpochHistoryToLEO(history, leo)
	}

	r.state.Role = RoleFollower
	r.state.Epoch = checkpoint.Epoch
	r.state.OffsetEpoch = offsetEpochForLEO(history, leo)
	r.state.LogStartOffset = checkpoint.LogStartOffset
	r.state.HW = checkpoint.HW
	r.state.LEO = leo
	r.epochHistory = append([]EpochPoint(nil), history...)
	r.recovered = true
	return nil
}

func validateCheckpoint(checkpoint Checkpoint) error {
	if checkpoint.LogStartOffset > checkpoint.HW {
		return ErrCorruptState
	}
	return nil
}

func validateEpochHistory(history []EpochPoint) error {
	for i, point := range history {
		if point.Epoch == 0 {
			return ErrCorruptState
		}
		if i == 0 {
			continue
		}
		prev := history[i-1]
		if point.Epoch < prev.Epoch {
			return ErrCorruptState
		}
		if point.StartOffset < prev.StartOffset {
			return ErrCorruptState
		}
		if point.Epoch == prev.Epoch && point.StartOffset != prev.StartOffset {
			return ErrCorruptState
		}
	}
	return nil
}

func (r *replica) InstallSnapshot(ctx context.Context, snap Snapshot) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state.Role == RoleTombstoned {
		return ErrTombstoned
	}
	if err := r.snapshots.InstallSnapshot(ctx, snap); err != nil {
		return err
	}

	checkpoint := Checkpoint{
		Epoch:          snap.Epoch,
		LogStartOffset: snap.EndOffset,
		HW:             snap.EndOffset,
	}
	if err := r.checkpoints.Store(checkpoint); err != nil {
		return err
	}

	leo := r.log.LEO()
	r.state.Role = RoleFollower
	r.state.Epoch = snap.Epoch
	r.state.OffsetEpoch = snap.Epoch
	r.state.LogStartOffset = snap.EndOffset
	r.state.HW = snap.EndOffset
	r.state.LEO = leo
	if leo < snap.EndOffset {
		return ErrCorruptState
	}
	return nil
}
