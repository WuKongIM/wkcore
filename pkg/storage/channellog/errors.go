package channellog

import "errors"

var (
	ErrInvalidConfig           = errors.New("channellog: invalid config")
	ErrConflictingMeta         = errors.New("channellog: conflicting metadata")
	ErrStaleMeta               = errors.New("channellog: stale metadata")
	ErrNotLeader               = errors.New("channellog: not leader")
	ErrChannelDeleting         = errors.New("channellog: channel deleting")
	ErrChannelNotFound         = errors.New("channellog: channel not found")
	ErrIdempotencyConflict     = errors.New("channellog: idempotency conflict")
	ErrProtocolUpgradeRequired = errors.New("channellog: protocol upgrade required")
	ErrMessageSeqExhausted     = errors.New("channellog: legacy message seq exhausted")
	ErrInvalidFetchArgument    = errors.New("channellog: invalid fetch argument")
	ErrInvalidFetchBudget      = errors.New("channellog: invalid fetch budget")

	errNotImplemented = errors.New("channellog: not implemented")
)
