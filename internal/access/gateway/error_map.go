package gateway

import (
	"context"
	"errors"

	runtimechannelid "github.com/WuKongIM/WuKongIM/internal/runtime/channelid"
	channellog "github.com/WuKongIM/WuKongIM/pkg/channel/log"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

func mapSendErrorReason(err error) (frame.ReasonCode, bool) {
	switch {
	case errors.Is(err, channellog.ErrChannelNotFound):
		return frame.ReasonChannelNotExist, true
	case errors.Is(err, channellog.ErrChannelDeleting):
		return frame.ReasonChannelDeleting, true
	case errors.Is(err, channellog.ErrProtocolUpgradeRequired):
		return frame.ReasonProtocolUpgradeRequired, true
	case errors.Is(err, channellog.ErrIdempotencyConflict):
		return frame.ReasonIdempotencyConflict, true
	case errors.Is(err, channellog.ErrMessageSeqExhausted):
		return frame.ReasonMessageSeqExhausted, true
	case errors.Is(err, channellog.ErrStaleMeta), errors.Is(err, channellog.ErrNotLeader):
		return frame.ReasonNodeNotMatch, true
	case errors.Is(err, runtimechannelid.ErrInvalidPersonChannel):
		return frame.ReasonChannelIDError, true
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return frame.ReasonSystemError, true
	default:
		return 0, false
	}
}
