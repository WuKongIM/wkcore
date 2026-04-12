package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/replica"
)

type taskMask uint8

const (
	taskReplication taskMask = 1 << iota
	taskSnapshot
)

type ChannelDelegate interface {
	OnReplication(key core.ChannelKey)
	OnSnapshot(key core.ChannelKey)
}

type channel struct {
	key      core.ChannelKey
	gen      uint64
	replica  replica.Replica
	now      func() time.Time
	delegate ChannelDelegate
	meta     atomic.Pointer[core.Meta]
	mu       sync.Mutex
	pending  taskMask

	replicationPeers nodeIDQueue
	snapshotBytes    int64
}

func newChannel(
	key core.ChannelKey,
	generation uint64,
	rep replica.Replica,
	meta core.Meta,
	now func() time.Time,
	delegate ChannelDelegate,
) *channel {
	if now == nil {
		now = time.Now
	}
	c := &channel{
		key:      key,
		gen:      generation,
		replica:  rep,
		now:      now,
		delegate: delegate,
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

func (c *channel) markReplication() {
	c.markTask(taskReplication)
}

func (c *channel) markSnapshot() {
	c.markTask(taskSnapshot)
}

func (c *channel) markTask(mask taskMask) {
	c.mu.Lock()
	c.pending |= mask
	c.mu.Unlock()
}

func (c *channel) runPendingTasks() {
	if c.delegate == nil {
		return
	}
	c.runTask(taskReplication, c.delegate.OnReplication)
	c.runTask(taskSnapshot, c.delegate.OnSnapshot)
}

func (c *channel) runTask(mask taskMask, fn func(core.ChannelKey)) {
	if fn == nil {
		return
	}
	c.mu.Lock()
	if c.pending&mask == 0 {
		c.mu.Unlock()
		return
	}
	c.pending &^= mask
	c.mu.Unlock()
	fn(c.key)
}

func (c *channel) enqueueReplication(peer core.NodeID) {
	c.mu.Lock()
	c.replicationPeers.enqueue(peer)
	c.mu.Unlock()
}

func (c *channel) popReplicationPeer() (core.NodeID, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.replicationPeers.pop()
}

func (c *channel) enqueueSnapshot(bytes int64) {
	c.mu.Lock()
	c.snapshotBytes += bytes
	c.mu.Unlock()
}

func (c *channel) drainSnapshotBytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	bytes := c.snapshotBytes
	c.snapshotBytes = 0
	return bytes
}

type nodeIDQueue struct {
	items []core.NodeID
	head  int
	set   map[core.NodeID]struct{}
	dirty map[core.NodeID]struct{}
}

func (q *nodeIDQueue) enqueue(nodeID core.NodeID) {
	if q.set == nil {
		q.set = make(map[core.NodeID]struct{})
	}
	if _, ok := q.set[nodeID]; ok {
		if q.dirty == nil {
			q.dirty = make(map[core.NodeID]struct{})
		}
		q.dirty[nodeID] = struct{}{}
		return
	}
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	} else if q.head > 0 && len(q.items) == cap(q.items) {
		q.compact()
	}
	q.items = append(q.items, nodeID)
	q.set[nodeID] = struct{}{}
}

func (q *nodeIDQueue) pop() (core.NodeID, bool) {
	if q.head >= len(q.items) {
		return 0, false
	}

	nodeID := q.items[q.head]
	q.items[q.head] = 0
	q.head++
	if _, ok := q.dirty[nodeID]; ok {
		delete(q.dirty, nodeID)
		if q.head == len(q.items) {
			q.items = q.items[:0]
			q.head = 0
		} else if q.head > 0 && len(q.items) == cap(q.items) {
			q.compact()
		}
		q.items = append(q.items, nodeID)
		return nodeID, true
	}
	delete(q.set, nodeID)
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	}
	return nodeID, true
}

func (q *nodeIDQueue) compact() {
	n := copy(q.items, q.items[q.head:])
	for i := n; i < len(q.items); i++ {
		q.items[i] = 0
	}
	q.items = q.items[:n]
	q.head = 0
}
