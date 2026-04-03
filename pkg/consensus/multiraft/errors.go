package multiraft

import "errors"

var (
	ErrInvalidOptions      = errors.New("multiraft: invalid options")
	ErrGroupExists         = errors.New("multiraft: group already exists")
	ErrGroupNotFound       = errors.New("multiraft: group not found")
	ErrGroupClosed         = errors.New("multiraft: group closed")
	ErrRuntimeClosed       = errors.New("multiraft: runtime closed")
	ErrNotLeader           = errors.New("multiraft: not leader")
	ErrConfigChangePending = errors.New("multiraft: config change pending")
	errNotImplemented      = errors.New("multiraft: not implemented")
)
