package runtime

import (
	"sync"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
)

type Priority uint8

const (
	PriorityHigh Priority = iota
	PriorityNormal
	PriorityLow
	priorityCount
)

type schedulerEntry struct {
	key      core.ChannelKey
	priority Priority
}

type scheduler struct {
	mu sync.Mutex

	queues     [priorityCount]schedulerQueue
	queued     map[core.ChannelKey]Priority
	processing map[core.ChannelKey]struct{}
	dirty      map[core.ChannelKey]Priority
}

func newScheduler() *scheduler {
	return &scheduler{
		queued:     make(map[core.ChannelKey]Priority),
		processing: make(map[core.ChannelKey]struct{}),
		dirty:      make(map[core.ChannelKey]Priority),
	}
}

func (s *scheduler) enqueue(key core.ChannelKey, priority Priority) {
	if priority >= priorityCount {
		priority = PriorityNormal
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, running := s.processing[key]; running {
		if queuedPriority, ok := s.dirty[key]; !ok || higherPriority(priority, queuedPriority) {
			s.dirty[key] = priority
		}
		return
	}
	if queuedPriority, ok := s.queued[key]; ok {
		if higherPriority(priority, queuedPriority) {
			s.queued[key] = priority
			s.queues[priority].enqueue(key)
		}
		return
	}
	s.queued[key] = priority
	s.queues[priority].enqueue(key)
}

func (s *scheduler) popReady() (schedulerEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := PriorityHigh; i < priorityCount; i++ {
		for {
			key, ok := s.queues[i].pop()
			if !ok {
				break
			}
			priority, queued := s.queued[key]
			if !queued || priority != i {
				continue
			}
			if _, running := s.processing[key]; running {
				continue
			}
			delete(s.queued, key)
			return schedulerEntry{key: key, priority: i}, true
		}
	}
	return schedulerEntry{}, false
}

func (s *scheduler) begin(key core.ChannelKey) {
	s.mu.Lock()
	s.processing[key] = struct{}{}
	s.mu.Unlock()
}

func (s *scheduler) done(key core.ChannelKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.processing, key)
	_, dirty := s.dirty[key]
	return dirty
}

func (s *scheduler) requeue(key core.ChannelKey) {
	s.mu.Lock()
	defer s.mu.Unlock()

	priority, ok := s.dirty[key]
	if !ok {
		return
	}
	delete(s.dirty, key)
	if _, queued := s.queued[key]; queued {
		return
	}
	s.queued[key] = priority
	s.queues[priority].enqueue(key)
}

func (s *scheduler) isDirty(key core.ChannelKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.dirty[key]
	return ok
}

func higherPriority(left, right Priority) bool {
	return left < right
}

type schedulerQueue struct {
	items []core.ChannelKey
	head  int
}

func (q *schedulerQueue) enqueue(key core.ChannelKey) {
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	} else if q.head > 0 && len(q.items) == cap(q.items) {
		q.compact()
	}
	q.items = append(q.items, key)
}

func (q *schedulerQueue) pop() (core.ChannelKey, bool) {
	if q.head >= len(q.items) {
		return "", false
	}

	key := q.items[q.head]
	q.items[q.head] = ""
	q.head++
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	}
	return key, true
}

func (q *schedulerQueue) compact() {
	n := copy(q.items, q.items[q.head:])
	for i := n; i < len(q.items); i++ {
		q.items[i] = ""
	}
	q.items = q.items[:n]
	q.head = 0
}

func (r *runtime) processChannel(key core.ChannelKey) {
	ch, ok := r.lookupChannel(key)
	if !ok {
		return
	}
	ch.runPendingTasks()
}

func (r *runtime) runScheduler() {
	for {
		entry, ok := r.scheduler.popReady()
		if !ok {
			return
		}
		r.scheduler.begin(entry.key)
		r.processChannel(entry.key)
		if r.scheduler.done(entry.key) {
			r.scheduler.requeue(entry.key)
		}
	}
}

func (r *runtime) enqueueScheduler(key core.ChannelKey, priority Priority) {
	r.scheduler.enqueue(key, priority)
	if r.cfg.AutoRunScheduler {
		go r.runScheduler()
	}
}
