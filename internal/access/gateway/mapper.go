package gateway

import (
	coregateway "github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

func mapSendCommand(ctx *coregateway.Context, pkt *wkpacket.SendPacket) (message.SendCommand, error) {
	if ctx == nil || ctx.Session == nil {
		return message.SendCommand{}, ErrUnauthenticatedSession
	}

	senderUID, _ := ctx.Session.Value(coregateway.SessionValueUID).(string)
	if senderUID == "" {
		return message.SendCommand{}, ErrUnauthenticatedSession
	}

	if pkt == nil {
		return message.SendCommand{SenderUID: senderUID}, nil
	}

	return message.SendCommand{
		Framer:      pkt.Framer,
		Setting:     pkt.Setting,
		MsgKey:      pkt.MsgKey,
		Expire:      pkt.Expire,
		SenderUID:   senderUID,
		ClientSeq:   pkt.ClientSeq,
		ClientMsgNo: pkt.ClientMsgNo,
		StreamNo:    pkt.StreamNo,
		ChannelID:   pkt.ChannelID,
		ChannelType: pkt.ChannelType,
		Topic:       pkt.Topic,
		Payload:     pkt.Payload,
	}, nil
}

func mapRecvAckCommand(ctx *coregateway.Context, pkt *wkpacket.RecvackPacket) (message.RecvAckCommand, error) {
	if ctx == nil || ctx.Session == nil {
		return message.RecvAckCommand{}, ErrUnauthenticatedSession
	}

	uid, _ := ctx.Session.Value(coregateway.SessionValueUID).(string)
	if uid == "" {
		return message.RecvAckCommand{}, ErrUnauthenticatedSession
	}

	if pkt == nil {
		return message.RecvAckCommand{UID: uid}, nil
	}

	return message.RecvAckCommand{
		UID:        uid,
		Framer:     pkt.Framer,
		MessageID:  pkt.MessageID,
		MessageSeq: pkt.MessageSeq,
	}, nil
}

func writeSendack(ctx *coregateway.Context, pkt *wkpacket.SendPacket, result message.SendResult) error {
	if ctx == nil || ctx.Session == nil {
		return ErrUnauthenticatedSession
	}

	var clientSeq uint64
	var clientMsgNo string
	if pkt != nil {
		clientSeq = pkt.ClientSeq
		clientMsgNo = pkt.ClientMsgNo
	}

	return ctx.WriteFrame(&wkpacket.SendackPacket{
		MessageID:   result.MessageID,
		MessageSeq:  result.MessageSeq,
		ClientSeq:   clientSeq,
		ClientMsgNo: clientMsgNo,
		ReasonCode:  result.Reason,
	})
}
