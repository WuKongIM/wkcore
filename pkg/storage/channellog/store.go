package channellog

import (
	"sync"
	"sync/atomic"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

type Store struct {
	db       *DB
	key      ChannelKey
	groupKey isr.GroupKey

	writeMu   sync.Mutex
	mu        sync.Mutex
	cachedLEO uint64
	leoLoaded bool

	writeInProgress atomic.Bool
	durableCommitCount atomic.Int64
}

func (s *Store) recordDurableCommit() {
	if s == nil {
		return
	}
	s.durableCommitCount.Add(1)
}
