package online

import "github.com/WuKongIM/WuKongIM/pkg/wkpacket"

type LocalDelivery struct{}

func (LocalDelivery) Deliver(recipients []OnlineConn, frame wkpacket.Frame) error {
	var firstErr error
	for _, recipient := range recipients {
		if recipient.Session == nil {
			if firstErr == nil {
				firstErr = ErrInvalidConnection
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
