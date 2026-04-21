package runtime

import (
	"sync"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
)

// laneDispatchWorkKey identifies a single peer/lane work item.
type laneDispatchWorkKey struct {
	peer core.NodeID
	lane uint16
}

// laneDispatchQueue serializes peer/lane work and tracks dirty requeues.
type laneDispatchQueue struct {
	mu sync.Mutex

	queue      []laneDispatchWorkKey
	head       int
	queued     map[laneDispatchWorkKey]struct{}
	processing map[laneDispatchWorkKey]struct{}
	dirty      map[laneDispatchWorkKey]struct{}
}

// newLaneDispatchQueue constructs an empty lane dispatch queue.
func newLaneDispatchQueue() *laneDispatchQueue {
	return &laneDispatchQueue{
		queued:     make(map[laneDispatchWorkKey]struct{}),
		processing: make(map[laneDispatchWorkKey]struct{}),
		dirty:      make(map[laneDispatchWorkKey]struct{}),
	}
}

// schedule adds work unless it is already queued or currently running.
func (q *laneDispatchQueue) schedule(key laneDispatchWorkKey) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, running := q.processing[key]; running {
		q.dirty[key] = struct{}{}
		return
	}
	if _, ok := q.queued[key]; ok {
		return
	}
	q.queued[key] = struct{}{}
	q.queue = append(q.queue, key)
}

// pop returns the next queued work item in FIFO order.
func (q *laneDispatchQueue) pop() (laneDispatchWorkKey, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		if q.head >= len(q.queue) {
			return laneDispatchWorkKey{}, false
		}
		key := q.queue[q.head]
		q.queue[q.head] = laneDispatchWorkKey{}
		q.head++
		if _, ok := q.queued[key]; !ok {
			continue
		}
		delete(q.queued, key)
		q.processing[key] = struct{}{}
		if q.head == len(q.queue) {
			q.queue = q.queue[:0]
			q.head = 0
		}
		return key, true
	}
}

// finish marks work complete and requeues dirty items once.
func (q *laneDispatchQueue) finish(key laneDispatchWorkKey) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, running := q.processing[key]; !running {
		return false
	}
	delete(q.processing, key)
	if _, dirty := q.dirty[key]; !dirty {
		return false
	}
	delete(q.dirty, key)
	if _, ok := q.queued[key]; ok {
		return false
	}
	q.queued[key] = struct{}{}
	q.queue = append(q.queue, key)
	return true
}

// reset clears all queue state.
func (q *laneDispatchQueue) reset() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.queue = nil
	q.head = 0
	q.queued = make(map[laneDispatchWorkKey]struct{})
	q.processing = make(map[laneDispatchWorkKey]struct{})
	q.dirty = make(map[laneDispatchWorkKey]struct{})
}
