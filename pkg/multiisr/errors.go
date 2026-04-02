package multiisr

import "errors"

var (
	ErrInvalidConfig      = errors.New("multiisr: invalid config")
	ErrTooManyGroups      = errors.New("multiisr: too many groups")
	ErrGroupNotFound      = errors.New("multiisr: group not found")
	ErrGenerationMismatch = errors.New("multiisr: generation mismatch")
	ErrBackpressured      = errors.New("multiisr: backpressured")

	errNotImplemented = errors.New("multiisr: not implemented")
)
