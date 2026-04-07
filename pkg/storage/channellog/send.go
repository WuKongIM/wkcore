package channellog

import (
	"context"
	"errors"
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

const maxLegacyMessageSeq = uint64(^uint32(0))

func (c *cluster) Append(ctx context.Context, req AppendRequest) (AppendResult, error) {
	draft := req.Message
	draft.ChannelID = req.ChannelID
	draft.ChannelType = req.ChannelType

	key := ChannelKey{
		ChannelID:   req.ChannelID,
		ChannelType: req.ChannelType,
	}
	groupKey := channelGroupKey(key)

	meta, err := c.metaForKey(key)
	if err != nil {
		return AppendResult{}, err
	}
	if err := compatibleWithExpectation(meta, req.ExpectedChannelEpoch, req.ExpectedLeaderEpoch); err != nil {
		return AppendResult{}, err
	}
	switch meta.Status {
	case ChannelStatusDeleting:
		return AppendResult{}, ErrChannelDeleting
	case ChannelStatusDeleted:
		return AppendResult{}, ErrChannelNotFound
	}
	if meta.Features.MessageSeqFormat == MessageSeqFormatU64 && !req.SupportsMessageSeqU64 {
		return AppendResult{}, ErrProtocolUpgradeRequired
	}

	group, ok := c.cfg.Runtime.Group(groupKey)
	if !ok {
		return AppendResult{}, ErrStaleMeta
	}
	state := group.Status()
	if state.Role != isr.RoleLeader {
		return AppendResult{}, ErrNotLeader
	}
	if meta.Features.MessageSeqFormat == MessageSeqFormatLegacyU32 && state.HW >= maxLegacyMessageSeq {
		return AppendResult{}, ErrMessageSeqExhausted
	}

	store, err := c.cfg.States.ForChannel(key)
	if err != nil {
		return AppendResult{}, err
	}
	if draft.ClientMsgNo != "" {
		idKey := IdempotencyKey{
			ChannelID:   req.ChannelID,
			ChannelType: req.ChannelType,
			FromUID:     draft.FromUID,
			ClientMsgNo: draft.ClientMsgNo,
		}
		entry, ok, err := store.GetIdempotency(idKey)
		if err != nil {
			return AppendResult{}, err
		}
		if ok {
			view, err := c.loadMessageViewAtOffset(groupKey, entry.Offset)
			if err != nil {
				return AppendResult{}, err
			}
			if view.PayloadHash != hashPayload(draft.Payload) {
				return AppendResult{}, ErrIdempotencyConflict
			}
			message := view.Message
			message.MessageSeq = entry.MessageSeq
			return AppendResult{MessageID: message.MessageID, MessageSeq: message.MessageSeq, Message: message}, nil
		}
	}

	draft.MessageID = c.cfg.MessageIDs.Next()
	encoded, err := encodeMessage(draft)
	if err != nil {
		return AppendResult{}, err
	}

	commit, err := group.Append(ctx, []isr.Record{{
		Payload:   encoded,
		SizeBytes: len(encoded),
	}})
	if err != nil {
		if errors.Is(err, isr.ErrNotLeader) || errors.Is(err, isr.ErrLeaseExpired) {
			return AppendResult{}, ErrNotLeader
		}
		return AppendResult{}, err
	}

	messageSeq := commit.NextCommitHW
	if meta.Features.MessageSeqFormat == MessageSeqFormatLegacyU32 && messageSeq > maxLegacyMessageSeq {
		return AppendResult{}, ErrMessageSeqExhausted
	}
	currentMeta, err := c.metaForKey(key)
	if err != nil {
		return AppendResult{}, err
	}
	switch currentMeta.Status {
	case ChannelStatusDeleting:
		return AppendResult{}, ErrChannelDeleting
	case ChannelStatusDeleted:
		return AppendResult{}, ErrChannelNotFound
	}

	committed := draft
	committed.MessageSeq = messageSeq

	if draft.ClientMsgNo != "" {
		if err := store.PutIdempotency(IdempotencyKey{
			ChannelID:   req.ChannelID,
			ChannelType: req.ChannelType,
			FromUID:     draft.FromUID,
			ClientMsgNo: draft.ClientMsgNo,
		}, IdempotencyEntry{
			MessageID:  committed.MessageID,
			MessageSeq: messageSeq,
			Offset:     messageSeq - 1,
		}); err != nil {
			return AppendResult{}, err
		}
	}

	return AppendResult{MessageID: committed.MessageID, MessageSeq: messageSeq, Message: committed}, nil
}

func (c *cluster) metaForKey(key ChannelKey) (ChannelMeta, error) {
	c.mu.RLock()
	meta, ok := c.metas[key]
	c.mu.RUnlock()
	if !ok {
		return ChannelMeta{}, ErrStaleMeta
	}
	return meta, nil
}

func (c *cluster) loadMessageViewAtOffset(groupKey isr.GroupKey, offset uint64) (messageView, error) {
	records, err := c.cfg.Log.Read(groupKey, offset, 1, math.MaxInt)
	if err != nil {
		return messageView{}, err
	}
	if len(records) == 0 {
		return messageView{}, ErrStaleMeta
	}
	return decodeMessageView(records[0].Payload)
}

func (c *cluster) loadMessageAtOffset(groupKey isr.GroupKey, offset uint64) (Message, error) {
	view, err := c.loadMessageViewAtOffset(groupKey, offset)
	if err != nil {
		return Message{}, err
	}
	msg := view.Message
	msg.MessageSeq = offset + 1
	return msg, nil
}
