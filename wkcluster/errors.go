package wkcluster

import "errors"

var (
	ErrNoLeader        = errors.New("wkcluster: no leader for group")
	ErrNotLeader       = errors.New("wkcluster: not leader")
	ErrLeaderNotStable = errors.New("wkcluster: leader not stable after retries")
	ErrGroupNotFound   = errors.New("wkcluster: group not found")
	ErrTimeout         = errors.New("wkcluster: forward timeout")
	ErrNodeNotFound    = errors.New("wkcluster: node not found")
	ErrInvalidConfig   = errors.New("wkcluster: invalid config")
	ErrStopped         = errors.New("wkcluster: cluster stopped")
)
