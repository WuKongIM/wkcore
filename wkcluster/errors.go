package wkcluster

import "errors"

var (
	ErrNoLeader        = errors.New("wkcluster: no leader for group")
	ErrNotLeader       = errors.New("wkcluster: not leader")
	ErrLeaderNotStable = errors.New("wkcluster: leader not stable after retries")
	ErrGroupNotFound   = errors.New("wkcluster: group not found")
	ErrInvalidConfig   = errors.New("wkcluster: invalid config")
)
