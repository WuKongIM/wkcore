package node

import "errors"

var (
	ErrInvalidConfig      = errors.New("isrnode: invalid config")
	ErrTooManyGroups      = errors.New("isrnode: too many groups")
	ErrGroupExists        = errors.New("isrnode: group already exists")
	ErrGroupNotFound      = errors.New("isrnode: group not found")
	ErrGenerationMismatch = errors.New("isrnode: generation mismatch")
	ErrBackpressured      = errors.New("isrnode: backpressured")

	errNotImplemented = errors.New("isrnode: not implemented")
)
