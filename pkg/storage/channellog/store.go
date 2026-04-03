package channellog

import "github.com/WuKongIM/WuKongIM/pkg/replication/isr"

type Store struct {
	db       *DB
	key      ChannelKey
	groupKey isr.GroupKey
}
