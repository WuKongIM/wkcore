package gateway

import (
	"context"

	coregateway "github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
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
	if reason, err := decryptSendPacketIfNeeded(ctx, pkt); err != nil {
		fields := append([]wklog.Field{
			wklog.Event("access.gateway.frame.send_decrypt_failed"),
		}, gatewaySendFields(ctx, pkt.ChannelID, pkt.ChannelType)...)
		fields = append(fields, wklog.Error(err))
		h.frameLogger().Warn("reject encrypted send request", fields...)
		return writeSendack(ctx, pkt, message.SendResult{Reason: reason})
	}

	cmd, err := mapSendCommand(ctx, pkt)
	if err != nil {
		fields := append([]wklog.Field{
			wklog.Event("access.gateway.frame.send_rejected"),
		}, gatewaySendFields(ctx, pkt.ChannelID, pkt.ChannelType)...)
		fields = append(fields, wklog.Error(err))
		h.frameLogger().Warn("reject send request", fields...)
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
		fields := append([]wklog.Field{
			wklog.Event("access.gateway.frame.send_failed"),
			wklog.SourceModule("message.send"),
		}, gatewaySendFields(ctx, cmd.ChannelID, cmd.ChannelType)...)
		fields = append(fields, wklog.Error(err))
		h.frameLogger().Warn("send request failed", fields...)
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
