package store

import (
	"sync"
	"sync/atomic"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

type ChannelStore struct {
	engine *Engine
	key    channel.ChannelKey
	id     channel.ChannelID

	writeMu sync.Mutex
	mu      sync.Mutex
	leo     atomic.Uint64
	loaded  atomic.Bool

	writeInProgress    atomic.Bool
	durableCommitCount atomic.Int64
}

func (s *ChannelStore) recordDurableCommit() {
	if s == nil {
		return
	}
	s.durableCommitCount.Add(1)
}

func (s *ChannelStore) commitCoordinator() *commitCoordinator {
	if s == nil || s.engine == nil {
		return nil
	}
	return s.engine.commitCoordinator()
}
