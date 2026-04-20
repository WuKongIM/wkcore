package runtime

import (
	"context"
	"time"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
)

type fetchLongPollContextKey struct{}

// WithoutFetchLongPoll marks a fetch request context so ServeFetch returns
// immediately on empty results instead of parking the RPC in long-poll mode.
func WithoutFetchLongPoll(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, fetchLongPollContextKey{}, false)
}

// FetchLongPollEnabled reports whether ServeFetch should wait for replica state
// changes before returning an empty fetch response.
func FetchLongPollEnabled(ctx context.Context) bool {
	if ctx == nil {
		return true
	}
	enabled, ok := ctx.Value(fetchLongPollContextKey{}).(bool)
	if !ok {
		return true
	}
	return enabled
}

type followerCursorApplier interface {
	ApplyFollowerCursor(ctx context.Context, req core.ReplicaFollowerCursorUpdate) error
}

func (r *runtime) ServeLanePoll(ctx context.Context, req LanePollRequestEnvelope) (LanePollResponseEnvelope, error) {
	if !r.longPollEnabled() {
		return LanePollResponseEnvelope{Status: LanePollStatusClosed}, nil
	}
	if req.LaneCount != uint16(r.cfg.LongPollLaneCount) {
		return LanePollResponseEnvelope{
			Status:        LanePollStatusNeedReset,
			ResetRequired: true,
			ResetReason:   LanePollResetReasonLaneLayoutMismatch,
		}, nil
	}

	sessionKey := PeerLaneKey{Peer: req.ReplicaID, LaneID: req.LaneID}
	session, ok := r.leaderLanes.Session(sessionKey)
	if req.Op == LanePollOpOpen || !ok {
		session = newLeaderLaneSession(r.requestID.Add(1), 1)
		for _, member := range req.FullMembership {
			ch, exists := r.lookupChannel(member.ChannelKey)
			if !exists {
				continue
			}
			meta := ch.metaSnapshot()
			if meta.Leader != r.cfg.LocalNode || meta.Epoch != member.ChannelEpoch {
				continue
			}
			session.TrackChannel(member.ChannelKey, member.ChannelEpoch)
			if ch.Status().LEO > 0 {
				session.MarkDataReady(member.ChannelKey, member.ChannelEpoch)
			}
		}
		r.leaderLanes.RegisterSession(sessionKey, session)
	} else if req.SessionID != 0 && req.SessionEpoch != 0 {
		currentID, currentEpoch := session.Session()
		if req.SessionID != currentID || req.SessionEpoch != currentEpoch {
			return LanePollResponseEnvelope{
				LaneID:        req.LaneID,
				Status:        LanePollStatusNeedReset,
				ResetRequired: true,
				ResetReason:   LanePollResetReasonSessionEpochMismatch,
				SessionID:     currentID,
				SessionEpoch:  currentEpoch,
			}, nil
		}
	}

	budget := LanePollBudget{MaxBytes: req.MaxBytes, MaxChannels: req.MaxChannels}
	if budget.MaxBytes <= 0 {
		budget.MaxBytes = r.cfg.LongPollMaxBytes
	}
	if budget.MaxChannels <= 0 {
		budget.MaxChannels = r.cfg.LongPollMaxChannels
	}

	selectItems := func() (LeaderLanePollResult, *lanePollWaiter) {
		return session.Poll(req.CursorDelta, func(delta LaneCursorDelta) {
			r.applyFollowerCursor(delta, req.ReplicaID)
		}, budget, func(key core.ChannelKey, cursor LaneCursorDelta, mask laneReadyMask) (LeaderLaneReadyItem, bool) {
			return r.selectLaneReadyItem(ctx, cursor, key, req.ReplicaID, budget.MaxBytes, mask)
		})
	}

	result, waiter := selectItems()
	if waiter == nil {
		sessionID, sessionEpoch := session.Session()
		resp := LanePollResponseEnvelope{
			LaneID:       req.LaneID,
			Status:       LanePollStatusOK,
			SessionID:    sessionID,
			SessionEpoch: sessionEpoch,
			MoreReady:    result.MoreReady,
			Items:        make([]LaneResponseItem, len(result.Items)),
		}
		for i, item := range result.Items {
			resp.Items[i] = itemToLaneResponse(item)
		}
		return resp, nil
	}

	maxWait := req.MaxWait
	if maxWait <= 0 {
		maxWait = r.cfg.LongPollMaxWait
	}
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return LanePollResponseEnvelope{}, ctx.Err()
	case <-waiter.Ready():
		result, _ = selectItems()
	case <-timer.C:
		sessionID, sessionEpoch := session.Session()
		return LanePollResponseEnvelope{
			LaneID:       req.LaneID,
			Status:       LanePollStatusOK,
			SessionID:    sessionID,
			SessionEpoch: sessionEpoch,
			TimedOut:     true,
		}, nil
	}

	sessionID, sessionEpoch := session.Session()
	resp := LanePollResponseEnvelope{
		LaneID:       req.LaneID,
		Status:       LanePollStatusOK,
		SessionID:    sessionID,
		SessionEpoch: sessionEpoch,
		MoreReady:    result.MoreReady,
		Items:        make([]LaneResponseItem, len(result.Items)),
	}
	for i, item := range result.Items {
		resp.Items[i] = itemToLaneResponse(item)
	}
	return resp, nil
}

func (r *runtime) selectLaneReadyItem(ctx context.Context, cursor LaneCursorDelta, key core.ChannelKey, replicaID core.NodeID, maxBytes int, mask laneReadyMask) (LeaderLaneReadyItem, bool) {
	ch, ok := r.lookupChannel(key)
	if !ok {
		return LeaderLaneReadyItem{ChannelKey: key}, true
	}
	state := ch.Status()
	fetchResult, err := ch.replica.Fetch(ctx, core.ReplicaFetchRequest{
		ChannelKey:  key,
		Epoch:       state.Epoch,
		ReplicaID:   replicaID,
		FetchOffset: cursor.MatchOffset,
		OffsetEpoch: cursor.OffsetEpoch,
		MaxBytes:    maxBytes,
	})
	if err != nil {
		return LeaderLaneReadyItem{
			ChannelKey:   key,
			ChannelEpoch: state.Epoch,
			ReadyMask:    mask,
			SizeBytes:    0,
		}, true
	}

	flags := LanePollItemFlagHWOnly
	if len(fetchResult.Records) > 0 {
		flags = LanePollItemFlagData
	}
	if fetchResult.TruncateTo != nil {
		flags |= LanePollItemFlagTruncate
	}
	item := LeaderLaneReadyItem{
		ChannelKey:   key,
		ChannelEpoch: state.Epoch,
		ReadyMask:    mask,
		SizeBytes:    laneRecordsSize(fetchResult.Records),
		Response: LaneResponseItem{
			ChannelKey:   key,
			ChannelEpoch: state.Epoch,
			LeaderEpoch:  state.Epoch,
			Flags:        flags,
			Records:      fetchResult.Records,
			LeaderHW:     fetchResult.HW,
			TruncateTo:   fetchResult.TruncateTo,
		},
	}

	finished := len(fetchResult.Records) == 0
	if len(fetchResult.Records) > 0 && cursor.MatchOffset+uint64(len(fetchResult.Records)) >= state.LEO {
		finished = true
	}
	return item, finished
}

func itemToLaneResponse(item LeaderLaneReadyItem) LaneResponseItem {
	if item.Response.ChannelKey != "" {
		return item.Response
	}
	return LaneResponseItem{
		ChannelKey:   item.ChannelKey,
		ChannelEpoch: item.ChannelEpoch,
		Flags:        LanePollItemFlagHWOnly,
	}
}

func laneRecordsSize(records []core.Record) int {
	total := 0
	for _, record := range records {
		total += record.SizeBytes
	}
	return total
}

func (r *runtime) applyFollowerCursor(delta LaneCursorDelta, replicaID core.NodeID) {
	ch, ok := r.lookupChannel(delta.ChannelKey)
	if !ok {
		return
	}
	state := ch.Status()
	if state.Epoch != delta.ChannelEpoch {
		return
	}
	if applier, ok := ch.replica.(followerCursorApplier); ok {
		_ = applier.ApplyFollowerCursor(context.Background(), core.ReplicaFollowerCursorUpdate{
			ChannelKey:  delta.ChannelKey,
			Epoch:       delta.ChannelEpoch,
			ReplicaID:   replicaID,
			MatchOffset: delta.MatchOffset,
			OffsetEpoch: delta.OffsetEpoch,
		})
		return
	}
	_ = ch.replica.ApplyProgressAck(context.Background(), core.ReplicaProgressAckRequest{
		ChannelKey:  delta.ChannelKey,
		Epoch:       delta.ChannelEpoch,
		ReplicaID:   replicaID,
		MatchOffset: delta.MatchOffset,
	})
}
