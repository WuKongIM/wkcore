package gateway

import (
	coregateway "github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

func (h *Handler) OnFrame(ctx *coregateway.Context, frame wkpacket.Frame) error {
	switch pkt := frame.(type) {
	case *wkpacket.SendPacket:
		return h.handleSend(ctx, pkt)
	case *wkpacket.RecvackPacket:
		return h.handleRecvAck(ctx, pkt)
	case *wkpacket.PingPacket:
		return nil
	default:
		return ErrUnsupportedFrame
	}
}

func (h *Handler) handleSend(ctx *coregateway.Context, pkt *wkpacket.SendPacket) error {
	cmd, err := mapSendCommand(ctx, pkt)
	if err != nil {
		return err
	}

	result, err := h.messages.Send(cmd)
	if err != nil {
		if reason, ok := mapSendErrorReason(err); ok {
			return writeSendack(ctx, pkt, message.SendResult{Reason: reason})
		}
		return err
	}

	return writeSendack(ctx, pkt, result)
}

func (h *Handler) handleRecvAck(ctx *coregateway.Context, pkt *wkpacket.RecvackPacket) error {
	cmd, err := mapRecvAckCommand(ctx, pkt)
	if err != nil {
		return err
	}
	return h.messages.RecvAck(cmd)
}
