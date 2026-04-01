package service

import (
	"context"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

func (s *Service) handleSend(ctx *gateway.Context, pkt *wkpacket.SendPacket) error {
	senderUID, err := senderUIDFromContext(ctx)
	if err != nil {
		return err
	}

	if pkt.ChannelType != wkpacket.ChannelTypePerson {
		return s.writeSendack(ctx, pkt, 0, 0, wkpacket.ReasonNotSupportChannelType)
	}

	recipients := s.registry.SessionsByUID(pkt.ChannelID)
	if len(recipients) == 0 {
		return s.writeSendack(ctx, pkt, 0, 0, wkpacket.ReasonUserNotOnNode)
	}

	msgID := s.sequencer.NextMessageID()
	msgSeq := s.sequencer.NextUserSequence(pkt.ChannelID)
	recv := buildPersonRecvPacket(senderUID, pkt, msgID, msgSeq, s.opts.Now())

	if err := s.delivery.Deliver(context.Background(), recipients, recv); err != nil {
		return s.writeSendack(ctx, pkt, msgID, msgSeq, wkpacket.ReasonSystemError)
	}

	return s.writeSendack(ctx, pkt, msgID, msgSeq, wkpacket.ReasonSuccess)
}

func senderUIDFromContext(ctx *gateway.Context) (string, error) {
	if ctx == nil || ctx.Session == nil {
		return "", ErrUnauthenticatedSession
	}

	uid, _ := ctx.Session.Value(gateway.SessionValueUID).(string)
	if uid == "" {
		return "", ErrUnauthenticatedSession
	}

	return uid, nil
}

func (s *Service) writeSendack(ctx *gateway.Context, pkt *wkpacket.SendPacket, msgID int64, msgSeq uint32, reason wkpacket.ReasonCode) error {
	if ctx == nil || ctx.Session == nil {
		return ErrUnauthenticatedSession
	}

	return ctx.WriteFrame(&wkpacket.SendackPacket{
		MessageID:   msgID,
		MessageSeq:  msgSeq,
		ClientSeq:   pkt.ClientSeq,
		ClientMsgNo: pkt.ClientMsgNo,
		ReasonCode:  reason,
	})
}

func buildPersonRecvPacket(senderUID string, pkt *wkpacket.SendPacket, msgID int64, msgSeq uint32, now time.Time) *wkpacket.RecvPacket {
	framer := pkt.Framer
	framer.FrameType = wkpacket.RECV

	return &wkpacket.RecvPacket{
		Framer:      framer,
		Setting:     pkt.Setting,
		MsgKey:      pkt.MsgKey,
		Expire:      pkt.Expire,
		MessageID:   msgID,
		MessageSeq:  msgSeq,
		ClientMsgNo: pkt.ClientMsgNo,
		StreamNo:    pkt.StreamNo,
		Timestamp:   int32(now.Unix()),
		ChannelID:   senderUID,
		ChannelType: wkpacket.ChannelTypePerson,
		Topic:       pkt.Topic,
		FromUID:     senderUID,
		Payload:     pkt.Payload,
		ClientSeq:   pkt.ClientSeq,
	}
}
