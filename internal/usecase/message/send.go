package message

import (
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

func (a *App) Send(cmd SendCommand) (SendResult, error) {
	if cmd.SenderUID == "" {
		return SendResult{}, ErrUnauthenticatedSender
	}

	if cmd.ChannelType != wkpacket.ChannelTypePerson {
		return SendResult{Reason: wkpacket.ReasonNotSupportChannelType}, nil
	}

	target := resolveLocalPersonTarget(cmd)
	recipients := a.online.ConnectionsByUID(target.RecipientUID)
	if len(recipients) == 0 {
		return SendResult{Reason: wkpacket.ReasonUserNotOnNode}, nil
	}

	msgID := a.sequence.NextMessageID()
	msgSeq := a.sequence.NextChannelSequence(target.SequenceKey)
	recv := buildPersonRecvPacket(cmd, msgID, msgSeq, a.now())

	if err := a.delivery.Deliver(recipients, recv); err != nil {
		return SendResult{
			MessageID:  msgID,
			MessageSeq: msgSeq,
			Reason:     wkpacket.ReasonSystemError,
		}, nil
	}

	return SendResult{
		MessageID:  msgID,
		MessageSeq: msgSeq,
		Reason:     wkpacket.ReasonSuccess,
	}, nil
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

func buildPersonRecvPacket(cmd SendCommand, msgID int64, msgSeq uint32, now time.Time) *wkpacket.RecvPacket {
	framer := cmd.Framer
	framer.FrameType = wkpacket.RECV

	return &wkpacket.RecvPacket{
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
		ChannelType: wkpacket.ChannelTypePerson,
		Topic:       cmd.Topic,
		FromUID:     cmd.SenderUID,
		Payload:     cmd.Payload,
		ClientSeq:   cmd.ClientSeq,
	}
}
