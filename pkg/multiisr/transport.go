package multiisr

import (
	"context"
	"encoding/json"

	"github.com/WuKongIM/WuKongIM/pkg/isr"
)

type fetchResponsePayload struct {
	TruncateTo *uint64      `json:"truncate_to,omitempty"`
	LeaderHW   uint64       `json:"leader_hw"`
	Records    []isr.Record `json:"records,omitempty"`
}

func encodeFetchResponsePayload(payload fetchResponsePayload) ([]byte, error) {
	return json.Marshal(payload)
}

func decodeFetchResponsePayload(data []byte) (fetchResponsePayload, error) {
	if len(data) == 0 {
		return fetchResponsePayload{}, nil
	}
	var payload fetchResponsePayload
	err := json.Unmarshal(data, &payload)
	return payload, err
}

func (r *runtime) handleEnvelope(env Envelope) {
	var (
		g         *group
		knownDrop bool
	)

	r.mu.Lock()
	r.dropExpiredTombstonesLocked(r.cfg.Now())
	if active, ok := r.groups[env.GroupID]; ok && active.generation == env.Generation {
		g = active
	} else if generations, ok := r.tombstones[env.GroupID]; ok {
		if _, ok := generations[env.Generation]; ok {
			knownDrop = true
		}
	}
	r.mu.Unlock()

	if env.Kind == MessageKindFetchResponse && (g != nil || knownDrop) {
		r.releasePeerInflight(env.Peer)
		defer r.drainPeerQueue(env.Peer)
	}

	if g == nil || knownDrop {
		return
	}
	r.deliverEnvelope(g, env)
}

func (r *runtime) deliverEnvelope(g *group, env Envelope) {
	switch env.Kind {
	case MessageKindFetchResponse:
		meta := g.metaSnapshot()
		if env.Epoch != meta.Epoch {
			return
		}
		payload, err := decodeFetchResponsePayload(env.Payload)
		if err != nil {
			return
		}
		_ = g.replica.ApplyFetch(context.Background(), isr.ApplyFetchRequest{
			GroupID:    env.GroupID,
			Epoch:      env.Epoch,
			Leader:     env.Peer,
			TruncateTo: payload.TruncateTo,
			Records:    payload.Records,
			LeaderHW:   payload.LeaderHW,
		})
	}
}
