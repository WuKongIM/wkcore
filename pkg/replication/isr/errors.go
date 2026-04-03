package isr

import "errors"

var (
	ErrInvalidConfig      = errors.New("isr: invalid config")
	ErrInvalidMeta        = errors.New("isr: invalid metadata")
	ErrStaleMeta          = errors.New("isr: stale metadata")
	ErrNotLeader          = errors.New("isr: not leader")
	ErrLeaseExpired       = errors.New("isr: lease expired")
	ErrInsufficientISR    = errors.New("isr: insufficient isr")
	ErrTombstoned         = errors.New("isr: tombstoned")
	ErrSnapshotRequired   = errors.New("isr: snapshot required")
	ErrInvalidFetchBudget = errors.New("isr: invalid fetch budget")
	ErrCorruptState       = errors.New("isr: corrupt state")
	ErrEmptyState         = errors.New("isr: empty state")

	errNotImplemented = errors.New("isr: not implemented")
)
