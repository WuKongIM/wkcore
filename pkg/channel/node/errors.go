package node

import "errors"

var (
	ErrInvalidConfig      = errors.New("isrnode: invalid config")
	ErrTooManyChannels    = errors.New("isrnode: too many groups")
	ErrChannelExists      = errors.New("isrnode: group already exists")
	ErrChannelNotFound    = errors.New("isrnode: group not found")
	ErrGenerationMismatch = errors.New("isrnode: generation mismatch")
	ErrBackpressured      = errors.New("isrnode: backpressured")

	errNotImplemented = errors.New("isrnode: not implemented")
)
