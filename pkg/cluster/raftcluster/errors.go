package raftcluster

import "errors"

var (
	ErrNoLeader        = errors.New("raftcluster: no leader for group")
	ErrNotLeader       = errors.New("raftcluster: not leader")
	ErrLeaderNotStable = errors.New("raftcluster: leader not stable after retries")
	ErrGroupNotFound   = errors.New("raftcluster: group not found")
	ErrInvalidConfig   = errors.New("raftcluster: invalid config")
)
