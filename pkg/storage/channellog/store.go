package channellog

import (
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

type Store struct {
	db       *DB
	key      ChannelKey
	groupKey isr.GroupKey

	mu        sync.Mutex
	cachedLEO uint64
	leoLoaded bool
}
