package channelcluster

import "errors"

var (
	ErrInvalidConfig           = errors.New("channelcluster: invalid config")
	ErrConflictingMeta         = errors.New("channelcluster: conflicting metadata")
	ErrStaleMeta               = errors.New("channelcluster: stale metadata")
	ErrNotLeader               = errors.New("channelcluster: not leader")
	ErrChannelDeleting         = errors.New("channelcluster: channel deleting")
	ErrChannelNotFound         = errors.New("channelcluster: channel not found")
	ErrIdempotencyConflict     = errors.New("channelcluster: idempotency conflict")
	ErrProtocolUpgradeRequired = errors.New("channelcluster: protocol upgrade required")
	ErrMessageSeqExhausted     = errors.New("channelcluster: legacy message seq exhausted")
	ErrInvalidFetchArgument    = errors.New("channelcluster: invalid fetch argument")
	ErrInvalidFetchBudget      = errors.New("channelcluster: invalid fetch budget")

	errNotImplemented = errors.New("channelcluster: not implemented")
)
