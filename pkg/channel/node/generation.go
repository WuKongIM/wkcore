package node

import "github.com/WuKongIM/WuKongIM/pkg/channel/isr"

func (r *runtime) allocateGeneration(channelKey isr.ChannelKey) (uint64, error) {
	current, err := r.cfg.GenerationStore.Load(channelKey)
	if err != nil {
		return 0, err
	}
	next := current + 1
	if err := r.cfg.GenerationStore.Store(channelKey, next); err != nil {
		return 0, err
	}
	return next, nil
}
