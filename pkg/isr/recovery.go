package isr

import "errors"

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
		if err := r.log.Truncate(checkpoint.HW); err != nil {
			return err
		}
		if err := r.log.Sync(); err != nil {
			return err
		}
		leo = r.log.LEO()
		if leo != checkpoint.HW {
			return ErrCorruptState
		}
	}

	r.state.Role = RoleFollower
	r.state.Epoch = checkpoint.Epoch
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
