package node

import (
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
)

type tombstone struct {
	groupKey   isr.GroupKey
	generation uint64
	expiresAt  time.Time
}

func (r *runtime) groupLocked(groupKey isr.GroupKey) (*group, bool) {
	g, ok := r.groups[groupKey]
	return g, ok
}

func (r *runtime) putGroupLocked(g *group) {
	r.groups[g.id] = g
}

func (r *runtime) tombstoneGroupLocked(g *group) {
	generations, ok := r.tombstones[g.id]
	if !ok {
		generations = make(map[uint64]tombstone)
		r.tombstones[g.id] = generations
	}
	generations[g.generation] = tombstone{
		groupKey:   g.id,
		generation: g.generation,
		expiresAt:  r.cfg.Now().Add(r.cfg.Tombstones.TombstoneTTL),
	}
}

func (r *runtime) dropExpiredTombstonesLocked(now time.Time) {
	for groupKey, generations := range r.tombstones {
		for generation, stone := range generations {
			if now.Before(stone.expiresAt) {
				continue
			}
			delete(generations, generation)
		}
		if len(generations) == 0 {
			delete(r.tombstones, groupKey)
		}
	}
}
