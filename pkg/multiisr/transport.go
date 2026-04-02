package multiisr

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/isr"
)

func (r *runtime) handleEnvelope(env Envelope) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.dropExpiredTombstonesLocked(r.cfg.Now())

	g, ok := r.groups[env.GroupID]
	if ok && g.generation == env.Generation {
		r.deliverEnvelopeLocked(g, env)
		return
	}
	if generations, ok := r.tombstones[env.GroupID]; ok {
		if _, ok := generations[env.Generation]; ok {
			return
		}
	}
}

func (r *runtime) deliverEnvelopeLocked(g *group, env Envelope) {
	switch env.Kind {
	case MessageKindFetchResponse:
		_ = g.replica.ApplyFetch(context.Background(), isr.ApplyFetchRequest{
			GroupID: env.GroupID,
			Epoch:   env.Epoch,
			Leader:  env.Peer,
		})
	}
}
