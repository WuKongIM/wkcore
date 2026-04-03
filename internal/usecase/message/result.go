package message

import "github.com/WuKongIM/WuKongIM/pkg/proto/wkpacket"

type SendResult struct {
	MessageID  int64
	MessageSeq uint64
	Reason     wkpacket.ReasonCode
}
