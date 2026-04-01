package service

import (
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

type DeliveryPort interface {
	Deliver(recipients []SessionMeta, frame wkpacket.Frame) error
}

type localDelivery struct{}

func (localDelivery) Deliver(recipients []SessionMeta, frame wkpacket.Frame) error {
	var firstErr error
	for _, recipient := range recipients {
		if recipient.Session == nil {
			if firstErr == nil {
				firstErr = ErrUnauthenticatedSession
			}
			continue
		}
		if err := recipient.Session.WriteFrame(frame); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
