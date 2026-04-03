package gateway

import (
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/msgstore/channelcluster"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
)

func mapSendErrorReason(err error) (wkframe.ReasonCode, bool) {
	switch {
	case errors.Is(err, channelcluster.ErrChannelNotFound):
		return wkframe.ReasonChannelNotExist, true
	case errors.Is(err, channelcluster.ErrChannelDeleting):
		return wkframe.ReasonChannelDeleting, true
	case errors.Is(err, channelcluster.ErrProtocolUpgradeRequired):
		return wkframe.ReasonProtocolUpgradeRequired, true
	case errors.Is(err, channelcluster.ErrIdempotencyConflict):
		return wkframe.ReasonIdempotencyConflict, true
	case errors.Is(err, channelcluster.ErrMessageSeqExhausted):
		return wkframe.ReasonMessageSeqExhausted, true
	case errors.Is(err, channelcluster.ErrStaleMeta), errors.Is(err, channelcluster.ErrNotLeader):
		return wkframe.ReasonNodeNotMatch, true
	default:
		return 0, false
	}
}
