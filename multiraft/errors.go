package multiraft

import "errors"

var (
	ErrInvalidOptions = errors.New("multiraft: invalid options")
	errNotImplemented = errors.New("multiraft: not implemented")
)
