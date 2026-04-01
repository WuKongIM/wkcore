package message

import "github.com/WuKongIM/WuKongIM/pkg/wkpacket"

type SendResult struct {
	MessageID  int64
	MessageSeq uint32
	Reason     wkpacket.ReasonCode
}
