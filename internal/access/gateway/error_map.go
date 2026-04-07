package gateway

import (
	"context"
	"errors"

	runtimechannelid "github.com/WuKongIM/WuKongIM/internal/runtime/channelid"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
)

func mapSendErrorReason(err error) (wkframe.ReasonCode, bool) {
	switch {
	case errors.Is(err, channellog.ErrChannelNotFound):
		return wkframe.ReasonChannelNotExist, true
	case errors.Is(err, channellog.ErrChannelDeleting):
		return wkframe.ReasonChannelDeleting, true
	case errors.Is(err, channellog.ErrProtocolUpgradeRequired):
		return wkframe.ReasonProtocolUpgradeRequired, true
	case errors.Is(err, channellog.ErrIdempotencyConflict):
		return wkframe.ReasonIdempotencyConflict, true
	case errors.Is(err, channellog.ErrMessageSeqExhausted):
		return wkframe.ReasonMessageSeqExhausted, true
	case errors.Is(err, channellog.ErrStaleMeta), errors.Is(err, channellog.ErrNotLeader):
		return wkframe.ReasonNodeNotMatch, true
	case errors.Is(err, runtimechannelid.ErrInvalidPersonChannel):
		return wkframe.ReasonChannelIDError, true
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return wkframe.ReasonSystemError, true
	default:
		return 0, false
	}
}
