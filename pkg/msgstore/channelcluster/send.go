package channelcluster

import (
	"context"
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/consensus/isr"
)

const maxLegacyMessageSeq = uint64(^uint32(0))

func (c *cluster) Send(ctx context.Context, req SendRequest) (SendResult, error) {
	key := ChannelKey{
		ChannelID:   req.ChannelID,
		ChannelType: req.ChannelType,
	}

	meta, err := c.metaForKey(key)
	if err != nil {
		return SendResult{}, err
	}
	if err := compatibleWithExpectation(meta, req.ExpectedChannelEpoch, req.ExpectedLeaderEpoch); err != nil {
		return SendResult{}, err
	}
	switch meta.Status {
	case ChannelStatusDeleting:
		return SendResult{}, ErrChannelDeleting
	case ChannelStatusDeleted:
		return SendResult{}, ErrChannelNotFound
	}
	if meta.Features.MessageSeqFormat == MessageSeqFormatU64 && !req.SupportsMessageSeqU64 {
		return SendResult{}, ErrProtocolUpgradeRequired
	}

	group, ok := c.cfg.Runtime.Group(meta.GroupID)
	if !ok {
		return SendResult{}, ErrStaleMeta
	}
	state := group.Status()
	if state.Role != isr.RoleLeader {
		return SendResult{}, ErrNotLeader
	}
	if meta.Features.MessageSeqFormat == MessageSeqFormatLegacyU32 && state.HW >= maxLegacyMessageSeq {
		return SendResult{}, ErrMessageSeqExhausted
	}

	store, err := c.cfg.States.ForChannel(key)
	if err != nil {
		return SendResult{}, err
	}
	if req.ClientMsgNo != "" {
		idKey := IdempotencyKey{
			ChannelID:   req.ChannelID,
			ChannelType: req.ChannelType,
			SenderUID:   req.SenderUID,
			ClientMsgNo: req.ClientMsgNo,
		}
		entry, ok, err := store.GetIdempotency(idKey)
		if err != nil {
			return SendResult{}, err
		}
		if ok {
			match, err := c.idempotentPayloadMatches(meta.GroupID, entry.Offset, req.Payload)
			if err != nil {
				return SendResult{}, err
			}
			if !match {
				return SendResult{}, ErrIdempotencyConflict
			}
			return SendResult{
				MessageID:  entry.MessageID,
				MessageSeq: entry.MessageSeq,
			}, nil
		}
	}

	messageID := c.cfg.MessageIDs.Next()
	payloadHash := hashPayload(req.Payload)
	encoded, err := encodeStoredMessage(storedMessage{
		MessageID:   messageID,
		SenderUID:   req.SenderUID,
		ClientMsgNo: req.ClientMsgNo,
		PayloadHash: payloadHash,
		Payload:     req.Payload,
	})
	if err != nil {
		return SendResult{}, err
	}

	commit, err := group.Append(ctx, []isr.Record{{
		Payload:   encoded,
		SizeBytes: len(encoded),
	}})
	if err != nil {
		if errors.Is(err, isr.ErrNotLeader) || errors.Is(err, isr.ErrLeaseExpired) {
			return SendResult{}, ErrNotLeader
		}
		return SendResult{}, err
	}

	messageSeq := commit.NextCommitHW
	if meta.Features.MessageSeqFormat == MessageSeqFormatLegacyU32 && messageSeq > maxLegacyMessageSeq {
		return SendResult{}, ErrMessageSeqExhausted
	}
	currentMeta, err := c.metaForKey(key)
	if err != nil {
		return SendResult{}, err
	}
	switch currentMeta.Status {
	case ChannelStatusDeleting:
		return SendResult{}, ErrChannelDeleting
	case ChannelStatusDeleted:
		return SendResult{}, ErrChannelNotFound
	}

	if req.ClientMsgNo != "" {
		if err := store.PutIdempotency(IdempotencyKey{
			ChannelID:   req.ChannelID,
			ChannelType: req.ChannelType,
			SenderUID:   req.SenderUID,
			ClientMsgNo: req.ClientMsgNo,
		}, IdempotencyEntry{
			MessageID:  messageID,
			MessageSeq: messageSeq,
			Offset:     messageSeq - 1,
		}); err != nil {
			return SendResult{}, err
		}
	}

	return SendResult{
		MessageID:  messageID,
		MessageSeq: messageSeq,
	}, nil
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

func (c *cluster) idempotentPayloadMatches(groupID, offset uint64, payload []byte) (bool, error) {
	records, err := c.cfg.Log.Read(groupID, offset, 1, len(payload)+1024)
	if err != nil {
		return false, err
	}
	if len(records) == 0 {
		return false, ErrStaleMeta
	}
	message, err := decodeStoredMessage(records[0].Payload)
	if err != nil {
		return false, err
	}
	return message.PayloadHash == hashPayload(payload), nil
}
