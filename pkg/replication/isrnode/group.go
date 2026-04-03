package isrnode

import (
	"context"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

type group struct {
	id         isr.GroupKey
	generation uint64
	replica    isr.Replica
	now        func() time.Time

	mu      sync.Mutex
	meta    isr.GroupMeta
	pending taskMask

	replicationPeers nodeIDQueue
	snapshotBytes    int64

	onControl     func()
	onReplication func()
	onCommit      func()
	onLease       func()
	onSnapshot    func()
}

func (g *group) ID() isr.GroupKey {
	return g.id
}

func (g *group) Status() isr.ReplicaState {
	return g.replica.Status()
}

func (g *group) Append(ctx context.Context, records []isr.Record) (isr.CommitResult, error) {
	g.mu.Lock()
	meta := g.meta
	now := g.now
	replica := g.replica
	g.mu.Unlock()

	state := replica.Status()
	if state.Role == isr.RoleTombstoned {
		return isr.CommitResult{}, isr.ErrTombstoned
	}
	if state.Role == isr.RoleFencedLeader {
		return isr.CommitResult{}, isr.ErrLeaseExpired
	}
	if !meta.LeaseUntil.IsZero() && !now().Before(meta.LeaseUntil) {
		return isr.CommitResult{}, isr.ErrLeaseExpired
	}
	return g.replica.Append(ctx, records)
}

func (g *group) setMeta(meta isr.GroupMeta) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.meta = meta
}

func (g *group) metaSnapshot() isr.GroupMeta {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.meta
}

func (g *group) markControl() {
	g.markTask(taskControl)
}

func (g *group) markReplication() {
	g.markTask(taskReplication)
}

func (g *group) markCommit() {
	g.markTask(taskCommit)
}

func (g *group) markLease() {
	g.markTask(taskLease)
}

func (g *group) markSnapshot() {
	g.markTask(taskSnapshot)
}

func (g *group) markTask(mask taskMask) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pending |= mask
}

func (g *group) runPendingTasks() {
	g.runTask(taskControl, g.onControl)
	g.runTask(taskReplication, g.onReplication)
	g.runTask(taskCommit, g.onCommit)
	g.runTask(taskLease, g.onLease)
	g.runTask(taskSnapshot, g.onSnapshot)
}

func (g *group) runTask(mask taskMask, fn func()) {
	g.mu.Lock()
	if g.pending&mask == 0 {
		g.mu.Unlock()
		return
	}
	g.pending &^= mask
	g.mu.Unlock()

	if fn != nil {
		fn()
	}
}

func (g *group) enqueueReplication(peer isr.NodeID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.replicationPeers.enqueue(peer)
}

func (g *group) popReplicationPeer() (isr.NodeID, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.replicationPeers.pop()
}

func (g *group) enqueueSnapshot(bytes int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.snapshotBytes += bytes
}

func (g *group) drainSnapshotBytes() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	bytes := g.snapshotBytes
	g.snapshotBytes = 0
	return bytes
}

type nodeIDQueue struct {
	items []isr.NodeID
	head  int
}

func (q *nodeIDQueue) enqueue(nodeID isr.NodeID) {
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	} else if q.head > 0 && len(q.items) == cap(q.items) {
		q.compact()
	}
	q.items = append(q.items, nodeID)
}

func (q *nodeIDQueue) pop() (isr.NodeID, bool) {
	if q.head >= len(q.items) {
		return 0, false
	}

	nodeID := q.items[q.head]
	q.items[q.head] = 0
	q.head++
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
