package isrnode

import (
	"context"
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

func (r *runtime) handleEnvelope(env Envelope) {
	var (
		g         *group
		knownDrop bool
	)

	r.mu.Lock()
	r.dropExpiredTombstonesLocked(r.cfg.Now())
	if active, ok := r.groups[env.GroupKey]; ok && active.generation == env.Generation {
		g = active
	} else if generations, ok := r.tombstones[env.GroupKey]; ok {
		if _, ok := generations[env.Generation]; ok {
			knownDrop = true
		}
	}
	r.mu.Unlock()

	if env.Kind == MessageKindFetchResponse && knownDrop {
		r.releasePeerInflight(env.Peer)
		r.releaseGroupInflight(env.GroupKey, env.Peer)
		r.drainPeerQueue(env.Peer)
		return
	}
	if env.Kind == MessageKindFetchFailure {
		r.releasePeerInflight(env.Peer)
		r.releaseGroupInflight(env.GroupKey, env.Peer)
		if g != nil {
			r.retryReplication(env.GroupKey, env.Peer, true)
			r.scheduleFollowerReplication(env.GroupKey, env.Peer)
		}
		r.drainPeerQueue(env.Peer)
		return
	}

	if g == nil {
		return
	}

	if env.Kind == MessageKindFetchResponse {
		if r.deliverEnvelope(g, env) {
			r.releasePeerInflight(env.Peer)
			r.releaseGroupInflight(env.GroupKey, env.Peer)
			r.drainPeerQueue(env.Peer)
		}
		return
	}
	_ = r.deliverEnvelope(g, env)
}

func (r *runtime) deliverEnvelope(g *group, env Envelope) bool {
	switch env.Kind {
	case MessageKindFetchResponse:
		meta := g.metaSnapshot()
		if env.Epoch != meta.Epoch {
			return true
		}
		if env.FetchResponse == nil {
			return false
		}
		return r.applyFetchResponseEnvelope(g, env.Peer, *env.FetchResponse) == nil
	case MessageKindProgressAck:
		meta := g.metaSnapshot()
		if env.Epoch != meta.Epoch {
			return true
		}
		if env.ProgressAck == nil {
			return false
		}
		return r.applyProgressAckEnvelope(g, *env.ProgressAck) == nil
	}
	return true
}

func (r *runtime) applyFetchResponseEnvelope(g *group, peer isr.NodeID, env FetchResponseEnvelope) error {
	if err := g.replica.ApplyFetch(context.Background(), isr.ApplyFetchRequest{
		GroupKey:   env.GroupKey,
		Epoch:      env.Epoch,
		Leader:     peer,
		TruncateTo: env.TruncateTo,
		Records:    env.Records,
		LeaderHW:   env.LeaderHW,
	}); err != nil {
		return err
	}

	meta := g.metaSnapshot()
	if meta.Leader != r.cfg.LocalNode {
		if len(env.Records) > 0 || env.TruncateTo != nil {
			state := g.Status()
			if err := r.sendEnvelope(Envelope{
				Peer:       meta.Leader,
				GroupKey:   g.id,
				Epoch:      meta.Epoch,
				Generation: g.generation,
				RequestID:  r.requestID.Add(1),
				Kind:       MessageKindProgressAck,
				ProgressAck: &ProgressAckEnvelope{
					GroupKey:    g.id,
					Epoch:       meta.Epoch,
					Generation:  g.generation,
					ReplicaID:   r.cfg.LocalNode,
					MatchOffset: state.LEO,
				},
			}); err != nil && !errors.Is(err, ErrBackpressured) {
				r.retryReplication(g.id, meta.Leader, true)
			}
		}
		if len(env.Records) == 0 && env.TruncateTo == nil {
			r.scheduleFollowerReplication(g.id, meta.Leader)
			return nil
		}
		state := g.Status()
		err := r.sendEnvelope(Envelope{
			Peer:       meta.Leader,
			GroupKey:   g.id,
			Epoch:      meta.Epoch,
			Generation: g.generation,
			RequestID:  r.requestID.Add(1),
			Kind:       MessageKindFetchRequest,
			FetchRequest: &FetchRequestEnvelope{
				GroupKey:    g.id,
				Epoch:       meta.Epoch,
				Generation:  g.generation,
				ReplicaID:   r.cfg.LocalNode,
				FetchOffset: state.LEO,
				OffsetEpoch: state.OffsetEpoch,
				MaxBytes:    defaultFetchMaxBytes,
			},
		})
		if err != nil && !errors.Is(err, ErrBackpressured) {
			r.retryReplication(g.id, meta.Leader, true)
		}
	}
	return nil
}

func (r *runtime) applyProgressAckEnvelope(g *group, env ProgressAckEnvelope) error {
	return g.replica.ApplyProgressAck(context.Background(), isr.ProgressAckRequest{
		GroupKey:    env.GroupKey,
		Epoch:       env.Epoch,
		ReplicaID:   env.ReplicaID,
		MatchOffset: env.MatchOffset,
	})
}
