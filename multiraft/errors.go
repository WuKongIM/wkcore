package multiraft

import "errors"

var (
	ErrInvalidOptions = errors.New("multiraft: invalid options")
	ErrGroupExists    = errors.New("multiraft: group already exists")
	errNotImplemented = errors.New("multiraft: not implemented")
)
