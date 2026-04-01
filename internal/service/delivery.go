package service

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

type DeliveryPort interface {
	Deliver(ctx context.Context, recipients []SessionMeta, frame wkpacket.Frame) error
}

type localDelivery struct{}

func (localDelivery) Deliver(_ context.Context, recipients []SessionMeta, frame wkpacket.Frame) error {
	for _, recipient := range recipients {
		if recipient.Session == nil {
			return ErrUnauthenticatedSession
		}
		if err := recipient.Session.WriteFrame(frame); err != nil {
			return err
		}
	}
	return nil
}
