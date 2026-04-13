package app

import (
	"context"
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	channelhandler "github.com/WuKongIM/WuKongIM/pkg/channel/handler"
	channellog "github.com/WuKongIM/WuKongIM/pkg/channel/log"
)

type messageChannelClusterAdapter struct {
	cluster channellog.Cluster
}

func (a messageChannelClusterAdapter) ApplyMeta(meta channel.Meta) error {
	if a.cluster == nil {
		return nil
	}
	return mapLegacyChannelError(a.cluster.ApplyMeta(rootChannelMetaToLegacy(meta)))
}

func (a messageChannelClusterAdapter) Append(ctx context.Context, req channel.AppendRequest) (channel.AppendResult, error) {
	if a.cluster == nil {
		return channel.AppendResult{}, nil
	}
	result, err := a.cluster.Append(ctx, rootAppendRequestToLegacy(req))
	if err != nil {
		return channel.AppendResult{}, mapLegacyChannelError(err)
	}
	return legacyAppendResultToRoot(result), nil
}

type messageMetaRefresherAdapter struct {
	refresher *channelMetaSync
}

type legacyCommittedDispatcherAdapter struct {
	dispatcher interface {
		SubmitCommitted(ctx context.Context, msg channel.Message) error
	}
}

func (a messageMetaRefresherAdapter) RefreshChannelMeta(ctx context.Context, id channel.ChannelID) (channel.Meta, error) {
	if a.refresher == nil {
		return channel.Meta{}, nil
	}
	meta, err := a.refresher.RefreshChannelMeta(ctx, channellog.ChannelKey{
		ChannelID:   id.ID,
		ChannelType: id.Type,
	})
	if err != nil {
		return channel.Meta{}, mapLegacyChannelError(err)
	}
	return legacyChannelMetaToRoot(meta), nil
}

func (a legacyCommittedDispatcherAdapter) SubmitCommitted(ctx context.Context, msg channellog.Message) error {
	if a.dispatcher == nil {
		return nil
	}
	return a.dispatcher.SubmitCommitted(ctx, legacyChannelMessageToRoot(msg))
}

func rootAppendRequestToLegacy(req channel.AppendRequest) channellog.AppendRequest {
	return channellog.AppendRequest{
		ChannelID:             req.ChannelID.ID,
		ChannelType:           req.ChannelID.Type,
		Message:               rootChannelMessageToLegacy(req.Message),
		SupportsMessageSeqU64: req.SupportsMessageSeqU64,
		ExpectedChannelEpoch:  req.ExpectedChannelEpoch,
		ExpectedLeaderEpoch:   req.ExpectedLeaderEpoch,
	}
}

func legacyAppendResultToRoot(result channellog.AppendResult) channel.AppendResult {
	return channel.AppendResult{
		MessageID:  result.MessageID,
		MessageSeq: result.MessageSeq,
		Message:    legacyChannelMessageToRoot(result.Message),
	}
}

func rootChannelMessageToLegacy(msg channel.Message) channellog.Message {
	return channellog.Message{
		MessageID:   msg.MessageID,
		MessageSeq:  msg.MessageSeq,
		Framer:      msg.Framer,
		Setting:     msg.Setting,
		MsgKey:      msg.MsgKey,
		Expire:      msg.Expire,
		ClientSeq:   msg.ClientSeq,
		ClientMsgNo: msg.ClientMsgNo,
		StreamNo:    msg.StreamNo,
		StreamID:    msg.StreamID,
		StreamFlag:  msg.StreamFlag,
		Timestamp:   msg.Timestamp,
		ChannelID:   msg.ChannelID,
		ChannelType: msg.ChannelType,
		Topic:       msg.Topic,
		FromUID:     msg.FromUID,
		Payload:     msg.Payload,
	}
}

func legacyChannelMessageToRoot(msg channellog.Message) channel.Message {
	return channel.Message{
		MessageID:   msg.MessageID,
		MessageSeq:  msg.MessageSeq,
		Framer:      msg.Framer,
		Setting:     msg.Setting,
		MsgKey:      msg.MsgKey,
		Expire:      msg.Expire,
		ClientSeq:   msg.ClientSeq,
		ClientMsgNo: msg.ClientMsgNo,
		StreamNo:    msg.StreamNo,
		StreamID:    msg.StreamID,
		StreamFlag:  msg.StreamFlag,
		Timestamp:   msg.Timestamp,
		ChannelID:   msg.ChannelID,
		ChannelType: msg.ChannelType,
		Topic:       msg.Topic,
		FromUID:     msg.FromUID,
		Payload:     msg.Payload,
	}
}

func rootChannelMetaToLegacy(meta channel.Meta) channellog.ChannelMeta {
	return channellog.ChannelMeta{
		ChannelID:    meta.ID.ID,
		ChannelType:  meta.ID.Type,
		ChannelEpoch: meta.Epoch,
		LeaderEpoch:  meta.LeaderEpoch,
		Replicas:     rootNodeIDsToLegacy(meta.Replicas),
		ISR:          rootNodeIDsToLegacy(meta.ISR),
		Leader:       channellog.NodeID(meta.Leader),
		MinISR:       meta.MinISR,
		Status:       channellog.ChannelStatus(meta.Status),
		Features: channellog.ChannelFeatures{
			MessageSeqFormat: channellog.MessageSeqFormat(meta.Features.MessageSeqFormat),
		},
	}
}

func legacyChannelMetaToRoot(meta channellog.ChannelMeta) channel.Meta {
	id := channel.ChannelID{
		ID:   meta.ChannelID,
		Type: meta.ChannelType,
	}
	return channel.Meta{
		Key:         channelhandler.KeyFromChannelID(id),
		ID:          id,
		Epoch:       meta.ChannelEpoch,
		LeaderEpoch: meta.LeaderEpoch,
		Leader:      channel.NodeID(meta.Leader),
		Replicas:    legacyNodeIDsToRoot(meta.Replicas),
		ISR:         legacyNodeIDsToRoot(meta.ISR),
		MinISR:      meta.MinISR,
		Status:      channel.Status(meta.Status),
		Features: channel.Features{
			MessageSeqFormat: channel.MessageSeqFormat(meta.Features.MessageSeqFormat),
		},
	}
}

func rootNodeIDsToLegacy(ids []channel.NodeID) []channellog.NodeID {
	out := make([]channellog.NodeID, 0, len(ids))
	for _, id := range ids {
		out = append(out, channellog.NodeID(id))
	}
	return out
}

func legacyNodeIDsToRoot(ids []channellog.NodeID) []channel.NodeID {
	out := make([]channel.NodeID, 0, len(ids))
	for _, id := range ids {
		out = append(out, channel.NodeID(id))
	}
	return out
}

func mapLegacyChannelError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, channel.ErrInvalidConfig),
		errors.Is(err, channel.ErrInvalidArgument),
		errors.Is(err, channel.ErrConflictingMeta),
		errors.Is(err, channel.ErrStaleMeta),
		errors.Is(err, channel.ErrNotLeader),
		errors.Is(err, channel.ErrChannelDeleting),
		errors.Is(err, channel.ErrChannelNotFound),
		errors.Is(err, channel.ErrIdempotencyConflict),
		errors.Is(err, channel.ErrProtocolUpgradeRequired),
		errors.Is(err, channel.ErrMessageSeqExhausted),
		errors.Is(err, channel.ErrMessageNotFound),
		errors.Is(err, channel.ErrInvalidFetchArgument),
		errors.Is(err, channel.ErrInvalidFetchBudget),
		errors.Is(err, channel.ErrCorruptValue):
		return err
	case errors.Is(err, channellog.ErrInvalidConfig):
		return errors.Join(channel.ErrInvalidConfig, err)
	case errors.Is(err, channellog.ErrInvalidArgument):
		return errors.Join(channel.ErrInvalidArgument, err)
	case errors.Is(err, channellog.ErrConflictingMeta):
		return errors.Join(channel.ErrConflictingMeta, err)
	case errors.Is(err, channellog.ErrStaleMeta):
		return errors.Join(channel.ErrStaleMeta, err)
	case errors.Is(err, channellog.ErrNotLeader):
		return errors.Join(channel.ErrNotLeader, err)
	case errors.Is(err, channellog.ErrChannelDeleting):
		return errors.Join(channel.ErrChannelDeleting, err)
	case errors.Is(err, channellog.ErrChannelNotFound):
		return errors.Join(channel.ErrChannelNotFound, err)
	case errors.Is(err, channellog.ErrIdempotencyConflict):
		return errors.Join(channel.ErrIdempotencyConflict, err)
	case errors.Is(err, channellog.ErrProtocolUpgradeRequired):
		return errors.Join(channel.ErrProtocolUpgradeRequired, err)
	case errors.Is(err, channellog.ErrMessageSeqExhausted):
		return errors.Join(channel.ErrMessageSeqExhausted, err)
	case errors.Is(err, channellog.ErrMessageNotFound):
		return errors.Join(channel.ErrMessageNotFound, err)
	case errors.Is(err, channellog.ErrInvalidFetchArgument):
		return errors.Join(channel.ErrInvalidFetchArgument, err)
	case errors.Is(err, channellog.ErrInvalidFetchBudget):
		return errors.Join(channel.ErrInvalidFetchBudget, err)
	case errors.Is(err, channellog.ErrCorruptValue):
		return errors.Join(channel.ErrCorruptValue, err)
	default:
		return err
	}
}
