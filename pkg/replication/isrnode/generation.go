package isrnode

func (r *runtime) allocateGeneration(groupID uint64) (uint64, error) {
	current, err := r.cfg.GenerationStore.Load(groupID)
	if err != nil {
		return 0, err
	}
	next := current + 1
	if err := r.cfg.GenerationStore.Store(groupID, next); err != nil {
		return 0, err
	}
	return next, nil
}
