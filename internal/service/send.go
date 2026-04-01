package service

import (
	"github.com/WuKongIM/WuKongIM/internal/gateway"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

func (s *Service) handleSend(_ *gateway.Context, _ *wkpacket.SendPacket) error {
	return nil
}
