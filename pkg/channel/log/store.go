package log

import (
	"sync"
	"sync/atomic"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
)

type Store struct {
	db       *DB
	key      ChannelKey
	groupKey isr.GroupKey

	writeMu   sync.Mutex
	mu        sync.Mutex
	cachedLEO uint64
	leoLoaded bool

	writeInProgress    atomic.Bool
	durableCommitCount atomic.Int64
}

func (s *Store) recordDurableCommit() {
	if s == nil {
		return
	}
	s.durableCommitCount.Add(1)
}

func (s *Store) commitCoordinator() *commitCoordinator {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.commitCoordinator()
}
