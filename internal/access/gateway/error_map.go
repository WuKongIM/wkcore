package gateway

import (
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/channelcluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
)

func mapSendErrorReason(err error) (wkpacket.ReasonCode, bool) {
	switch {
	case errors.Is(err, channelcluster.ErrChannelNotFound):
		return wkpacket.ReasonChannelNotExist, true
	case errors.Is(err, channelcluster.ErrChannelDeleting):
		return wkpacket.ReasonChannelDeleting, true
	case errors.Is(err, channelcluster.ErrProtocolUpgradeRequired):
		return wkpacket.ReasonProtocolUpgradeRequired, true
	case errors.Is(err, channelcluster.ErrIdempotencyConflict):
		return wkpacket.ReasonIdempotencyConflict, true
	case errors.Is(err, channelcluster.ErrMessageSeqExhausted):
		return wkpacket.ReasonMessageSeqExhausted, true
	case errors.Is(err, channelcluster.ErrStaleMeta), errors.Is(err, channelcluster.ErrNotLeader):
		return wkpacket.ReasonNodeNotMatch, true
	default:
		return 0, false
	}
}
