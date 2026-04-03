package isrnode

import "github.com/WuKongIM/WuKongIM/pkg/replication/isr"

func (r *runtime) allocateGeneration(groupKey isr.GroupKey) (uint64, error) {
	current, err := r.cfg.GenerationStore.Load(groupKey)
	if err != nil {
		return 0, err
	}
	next := current + 1
	if err := r.cfg.GenerationStore.Store(groupKey, next); err != nil {
		return 0, err
	}
	return next, nil
}
