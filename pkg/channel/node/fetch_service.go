package node

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
)

var _ FetchService = (*runtime)(nil)

func (r *runtime) ServeFetch(ctx context.Context, req FetchRequestEnvelope) (FetchResponseEnvelope, error) {
	r.mu.Lock()
	r.dropExpiredTombstonesLocked(r.cfg.Now())
	g, ok := r.channelLocked(req.ChannelKey)
	r.mu.Unlock()
	if !ok {
		return FetchResponseEnvelope{}, ErrChannelNotFound
	}
	if g.generation != req.Generation {
		return FetchResponseEnvelope{}, ErrGenerationMismatch
	}

	meta := g.metaSnapshot()
	if req.Epoch != meta.Epoch {
		return FetchResponseEnvelope{}, isr.ErrStaleMeta
	}

	fetchReq := isr.FetchRequest{
		ChannelKey:  req.ChannelKey,
		Epoch:       req.Epoch,
		ReplicaID:   req.ReplicaID,
		FetchOffset: req.FetchOffset,
		OffsetEpoch: req.OffsetEpoch,
		MaxBytes:    req.MaxBytes,
	}
	result, err := g.replica.Fetch(ctx, fetchReq)
	if err != nil {
		return FetchResponseEnvelope{}, err
	}
	return FetchResponseEnvelope{
		ChannelKey: req.ChannelKey,
		Epoch:      result.Epoch,
		Generation: req.Generation,
		TruncateTo: result.TruncateTo,
		LeaderHW:   result.HW,
		Records:    result.Records,
	}, nil
}
