package message

import (
	"context"
	"time"

	runtimechannelid "github.com/WuKongIM/WuKongIM/internal/runtime/channelid"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
)

func (a *App) Send(ctx context.Context, cmd SendCommand) (SendResult, error) {
	if cmd.FromUID == "" {
		return SendResult{}, ErrUnauthenticatedSender
	}

	if cmd.ChannelType != wkframe.ChannelTypePerson && cmd.ChannelType != wkframe.ChannelTypeGroup {
		return SendResult{Reason: wkframe.ReasonNotSupportChannelType}, nil
	}
	if cmd.ChannelType == wkframe.ChannelTypePerson {
		channelID, err := runtimechannelid.NormalizePersonChannel(cmd.FromUID, cmd.ChannelID)
		if err != nil {
			return SendResult{}, err
		}
		cmd.ChannelID = channelID
	}

	if a.cluster == nil {
		return SendResult{}, ErrClusterRequired
	}

	return a.sendDurable(ctx, cmd)
}

func (a *App) sendDurable(ctx context.Context, cmd SendCommand) (SendResult, error) {
	draft := buildDurableMessage(cmd, a.now())
	result, err := sendWithMetaRefreshRetry(ctx, a.cluster, a.refresher, channellog.AppendRequest{
		ChannelID:             cmd.ChannelID,
		ChannelType:           cmd.ChannelType,
		Message:               draft,
		SupportsMessageSeqU64: supportsMessageSeqU64(cmd.ProtocolVersion),
		ExpectedChannelEpoch:  cmd.ExpectedChannelEpoch,
		ExpectedLeaderEpoch:   cmd.ExpectedLeaderEpoch,
	})
	if err != nil {
		return SendResult{}, err
	}

	sendResult := SendResult{
		MessageID:  int64(result.MessageID),
		MessageSeq: result.MessageSeq,
		Reason:     wkframe.ReasonSuccess,
	}

	if a.dispatcher != nil {
		_ = a.dispatcher.SubmitCommitted(ctx, result.Message)
	}
	return sendResult, nil
}

func buildDurableMessage(cmd SendCommand, now time.Time) channellog.Message {
	return channellog.Message{
		Framer:      cmd.Framer,
		Setting:     cmd.Setting,
		MsgKey:      cmd.MsgKey,
		Expire:      cmd.Expire,
		ClientSeq:   cmd.ClientSeq,
		ClientMsgNo: cmd.ClientMsgNo,
		StreamNo:    cmd.StreamNo,
		Timestamp:   int32(now.Unix()),
		ChannelID:   cmd.ChannelID,
		ChannelType: cmd.ChannelType,
		Topic:       cmd.Topic,
		FromUID:     cmd.FromUID,
		Payload:     append([]byte(nil), cmd.Payload...),
	}
}

func supportsMessageSeqU64(version uint8) bool {
	return version == 0 || version > wkframe.LegacyMessageSeqVersion
}
