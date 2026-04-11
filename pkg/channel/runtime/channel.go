package runtime

import (
	"context"
	"sync/atomic"
	"time"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/replica"
)

type channel struct {
	key     core.ChannelKey
	gen     uint64
	replica replica.Replica
	now     func() time.Time
	meta    atomic.Pointer[core.Meta]
}

func newChannel(key core.ChannelKey, generation uint64, rep replica.Replica, meta core.Meta, now func() time.Time) *channel {
	if now == nil {
		now = time.Now
	}
	c := &channel{
		key:     key,
		gen:     generation,
		replica: rep,
		now:     now,
	}
	c.setMeta(meta)
	return c
}

func (c *channel) ID() core.ChannelKey {
	return c.key
}

func (c *channel) Meta() core.Meta {
	return c.metaSnapshot()
}

func (c *channel) Status() core.ReplicaState {
	return c.replica.Status()
}

func (c *channel) Append(ctx context.Context, records []core.Record) (core.CommitResult, error) {
	meta := c.metaSnapshot()
	state := c.replica.Status()
	if state.Role == core.ReplicaRoleTombstoned {
		return core.CommitResult{}, core.ErrTombstoned
	}
	if state.Role == core.ReplicaRoleFencedLeader {
		return core.CommitResult{}, core.ErrLeaseExpired
	}
	if !meta.LeaseUntil.IsZero() && !c.now().Before(meta.LeaseUntil) {
		return core.CommitResult{}, core.ErrLeaseExpired
	}
	return c.replica.Append(ctx, records)
}

func (c *channel) setMeta(meta core.Meta) {
	next := meta
	c.meta.Store(&next)
}

func (c *channel) metaSnapshot() core.Meta {
	ptr := c.meta.Load()
	if ptr == nil {
		return core.Meta{}
	}
	return *ptr
}
