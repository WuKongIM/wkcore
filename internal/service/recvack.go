package service

import (
	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

func (s *Service) handleRecvack(_ *gateway.Context, _ *wkpacket.RecvackPacket) error {
	return nil
}
