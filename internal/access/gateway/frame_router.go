package gateway

import (
	"context"

	coregateway "github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

func (h *Handler) OnFrame(ctx *coregateway.Context, f frame.Frame) error {
	switch pkt := f.(type) {
	case *frame.SendPacket:
		return h.handleSend(ctx, pkt)
	case *frame.RecvackPacket:
		return h.handleRecvAck(ctx, pkt)
	case *frame.PingPacket:
		return nil
	default:
		return ErrUnsupportedFrame
	}
}

func (h *Handler) handleSend(ctx *coregateway.Context, pkt *frame.SendPacket) error {
	cmd, err := mapSendCommand(ctx, pkt)
	if err != nil {
		if reason, ok := mapSendErrorReason(err); ok {
			return writeSendack(ctx, pkt, message.SendResult{Reason: reason})
		}
		return err
	}

	if ctx == nil || ctx.RequestContext == nil {
		return ErrMissingRequestContext
	}
	reqCtx, cancel := context.WithTimeout(ctx.RequestContext, h.sendTimeout)
	defer cancel()

	result, err := h.messages.Send(reqCtx, cmd)
	if err != nil {
		if reason, ok := mapSendErrorReason(err); ok {
			result.Reason = reason
			return writeSendack(ctx, pkt, result)
		}
		return err
	}

	return writeSendack(ctx, pkt, result)
}

func (h *Handler) handleRecvAck(ctx *coregateway.Context, pkt *frame.RecvackPacket) error {
	cmd, err := mapRecvAckCommand(ctx, pkt)
	if err != nil {
		return err
	}
	return h.messages.RecvAck(cmd)
}
