package api

import (
	"errors"
	"net/http"

	"github.com/WuKongIM/WuKongIM/pkg/msgstore/channelcluster"
)

func mapSendError(err error) (int, string, bool) {
	switch {
	case errors.Is(err, channelcluster.ErrChannelNotFound):
		return http.StatusNotFound, "channel not found", true
	case errors.Is(err, channelcluster.ErrChannelDeleting):
		return http.StatusConflict, "channel deleting", true
	case errors.Is(err, channelcluster.ErrProtocolUpgradeRequired):
		return http.StatusUpgradeRequired, "protocol upgrade required", true
	case errors.Is(err, channelcluster.ErrIdempotencyConflict):
		return http.StatusConflict, "idempotency conflict", true
	case errors.Is(err, channelcluster.ErrMessageSeqExhausted):
		return http.StatusConflict, "message seq exhausted", true
	case errors.Is(err, channelcluster.ErrStaleMeta), errors.Is(err, channelcluster.ErrNotLeader):
		return http.StatusServiceUnavailable, "retry required", true
	default:
		return 0, "", false
	}
}
