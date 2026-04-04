package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/WuKongIM/WuKongIM/pkg/storage/channellog"
)

func mapSendError(err error) (int, string, bool) {
	switch {
	case errors.Is(err, channellog.ErrChannelNotFound):
		return http.StatusNotFound, "channel not found", true
	case errors.Is(err, channellog.ErrChannelDeleting):
		return http.StatusConflict, "channel deleting", true
	case errors.Is(err, channellog.ErrProtocolUpgradeRequired):
		return http.StatusUpgradeRequired, "protocol upgrade required", true
	case errors.Is(err, channellog.ErrIdempotencyConflict):
		return http.StatusConflict, "idempotency conflict", true
	case errors.Is(err, channellog.ErrMessageSeqExhausted):
		return http.StatusConflict, "message seq exhausted", true
	case errors.Is(err, channellog.ErrStaleMeta), errors.Is(err, channellog.ErrNotLeader):
		return http.StatusServiceUnavailable, "retry required", true
	case errors.Is(err, context.Canceled):
		return http.StatusRequestTimeout, "request canceled", true
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusRequestTimeout, "request timeout", true
	default:
		return 0, "", false
	}
}
