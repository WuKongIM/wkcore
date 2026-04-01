package service

import (
	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

func (s *Service) OnFrame(ctx *gateway.Context, frame wkpacket.Frame) error {
	switch pkt := frame.(type) {
	case *wkpacket.SendPacket:
		return s.handleSend(ctx, pkt)
	case *wkpacket.RecvackPacket:
		return s.handleRecvack(ctx, pkt)
	case *wkpacket.PingPacket:
		return nil
	default:
		return ErrUnsupportedFrame
	}
}
