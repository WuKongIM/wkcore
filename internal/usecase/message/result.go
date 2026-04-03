package message

import "github.com/WuKongIM/WuKongIM/pkg/wkpacket"

type SendResult struct {
	MessageID  int64
	MessageSeq uint64
	Reason     wkpacket.ReasonCode
}
