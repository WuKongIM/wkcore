package message

import "github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"

type SendResult struct {
	MessageID  int64
	MessageSeq uint64
	Reason     wkframe.ReasonCode
}
