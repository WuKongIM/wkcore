package isr

func (r *replica) appendEpochPointLocked(point EpochPoint) error {
	if len(r.epochHistory) > 0 {
		last := r.epochHistory[len(r.epochHistory)-1]
		switch {
		case point.Epoch > last.Epoch:
			if point.StartOffset < last.StartOffset {
				return ErrCorruptState
			}
		case point.Epoch == last.Epoch && point.StartOffset == last.StartOffset:
			return nil
		default:
			return ErrCorruptState
		}
	}

	if err := r.history.Append(point); err != nil {
		return err
	}
	r.epochHistory = append(r.epochHistory, point)
	return nil
}

func (r *replica) truncateLogToLocked(to uint64) error {
	if err := r.log.Truncate(to); err != nil {
		return err
	}
	if err := r.log.Sync(); err != nil {
		return err
	}
	if err := r.history.TruncateTo(to); err != nil {
		return err
	}
	leo := r.log.LEO()
	if leo != to {
		return ErrCorruptState
	}
	r.epochHistory = trimEpochHistoryToLEO(r.epochHistory, to)
	return nil
}

func trimEpochHistoryToLEO(history []EpochPoint, leo uint64) []EpochPoint {
	if len(history) == 0 {
		return nil
	}

	end := 0
	for end < len(history) && history[end].StartOffset <= leo {
		end++
	}
	if end == 0 {
		return nil
	}
	return append([]EpochPoint(nil), history[:end]...)
}

func offsetEpochForLEO(history []EpochPoint, leo uint64) uint64 {
	if len(history) == 0 {
		return 0
	}

	var epoch uint64
	for _, point := range history {
		if point.StartOffset > leo {
			break
		}
		epoch = point.Epoch
	}
	return epoch
}
