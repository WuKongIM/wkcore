package message

import (
	"context"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/msgstore/channelcluster"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
)

func (a *App) Send(cmd SendCommand) (SendResult, error) {
	if cmd.SenderUID == "" {
		return SendResult{}, ErrUnauthenticatedSender
	}

	if cmd.ChannelType != wkframe.ChannelTypePerson {
		return SendResult{Reason: wkframe.ReasonNotSupportChannelType}, nil
	}

	if a.cluster != nil {
		return a.sendDurablePerson(context.Background(), cmd)
	}

	return a.sendLocalPerson(cmd)
}

func (a *App) sendDurablePerson(ctx context.Context, cmd SendCommand) (SendResult, error) {
	result, err := sendWithMetaRefreshRetry(ctx, a.cluster, a.refresher, channelcluster.SendRequest{
		ChannelID:             cmd.ChannelID,
		ChannelType:           cmd.ChannelType,
		SenderUID:             cmd.SenderUID,
		ClientMsgNo:           cmd.ClientMsgNo,
		Payload:               cmd.Payload,
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

	// Durable ack follows the replicated write; local fanout is best-effort.
	_ = a.deliverLocalPerson(cmd, sendResult.MessageID, sendResult.MessageSeq)
	return sendResult, nil
}

func (a *App) sendLocalPerson(cmd SendCommand) (SendResult, error) {
	target := resolveLocalPersonTarget(cmd)
	recipients := a.online.ConnectionsByUID(target.RecipientUID)
	if len(recipients) == 0 {
		return SendResult{Reason: wkframe.ReasonUserNotOnNode}, nil
	}

	msgID := a.sequence.NextMessageID()
	msgSeq := uint64(a.sequence.NextChannelSequence(target.SequenceKey))
	if err := a.deliverLocalPerson(cmd, msgID, msgSeq); err != nil {
		return SendResult{
			MessageID:  msgID,
			MessageSeq: msgSeq,
			Reason:     wkframe.ReasonSystemError,
		}, nil
	}

	return SendResult{
		MessageID:  msgID,
		MessageSeq: msgSeq,
		Reason:     wkframe.ReasonSuccess,
	}, nil
}

func (a *App) deliverLocalPerson(cmd SendCommand, msgID int64, msgSeq uint64) error {
	recipients := a.online.ConnectionsByUID(cmd.ChannelID)
	if len(recipients) == 0 {
		return nil
	}

	return a.delivery.Deliver(recipients, buildPersonRecvPacket(cmd, msgID, msgSeq, a.now()))
}

type localPersonTarget struct {
	RecipientUID string
	SequenceKey  string
}

func resolveLocalPersonTarget(cmd SendCommand) localPersonTarget {
	return localPersonTarget{
		RecipientUID: cmd.ChannelID,
		SequenceKey:  cmd.ChannelID,
	}
}

func buildPersonRecvPacket(cmd SendCommand, msgID int64, msgSeq uint64, now time.Time) *wkframe.RecvPacket {
	framer := cmd.Framer
	framer.FrameType = wkframe.RECV

	return &wkframe.RecvPacket{
		Framer:      framer,
		Setting:     cmd.Setting,
		MsgKey:      cmd.MsgKey,
		Expire:      cmd.Expire,
		MessageID:   msgID,
		MessageSeq:  msgSeq,
		ClientMsgNo: cmd.ClientMsgNo,
		StreamNo:    cmd.StreamNo,
		Timestamp:   int32(now.Unix()),
		ChannelID:   cmd.SenderUID,
		ChannelType: wkframe.ChannelTypePerson,
		Topic:       cmd.Topic,
		FromUID:     cmd.SenderUID,
		Payload:     cmd.Payload,
		ClientSeq:   cmd.ClientSeq,
	}
}

func supportsMessageSeqU64(version uint8) bool {
	return version == 0 || version > wkframe.LegacyMessageSeqVersion
}
