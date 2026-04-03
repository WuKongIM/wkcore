package gateway

import (
	coregateway "github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
)

func (h *Handler) OnFrame(ctx *coregateway.Context, frame wkframe.Frame) error {
	switch pkt := frame.(type) {
	case *wkframe.SendPacket:
		return h.handleSend(ctx, pkt)
	case *wkframe.RecvackPacket:
		return h.handleRecvAck(ctx, pkt)
	case *wkframe.PingPacket:
		return nil
	default:
		return ErrUnsupportedFrame
	}
}

func (h *Handler) handleSend(ctx *coregateway.Context, pkt *wkframe.SendPacket) error {
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

func (h *Handler) handleRecvAck(ctx *coregateway.Context, pkt *wkframe.RecvackPacket) error {
	cmd, err := mapRecvAckCommand(ctx, pkt)
	if err != nil {
		return err
	}
	return h.messages.RecvAck(cmd)
}
