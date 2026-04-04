package isrnode

import (
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

type snapshotThrottle interface {
	Wait(bytes int64)
}

type fixedRateSnapshotThrottle struct {
	bytesPerSecond int64
	sleep          func(time.Duration)
}

func newSnapshotThrottle(bytesPerSecond int64, sleep func(time.Duration)) snapshotThrottle {
	return fixedRateSnapshotThrottle{
		bytesPerSecond: bytesPerSecond,
		sleep:          sleep,
	}
}

func (t fixedRateSnapshotThrottle) Wait(bytes int64) {
	if t.bytesPerSecond <= 0 || bytes <= 0 {
		return
	}
	delay := time.Duration(bytes*int64(time.Second)) / time.Duration(t.bytesPerSecond)
	if delay <= 0 {
		delay = time.Second
	}
	if t.sleep != nil {
		t.sleep(delay)
	}
}

type snapshotState struct {
	mu            sync.Mutex
	inflight      int
	maxConcurrent int
	waiting       []isr.GroupKey
	waitingSet    map[isr.GroupKey]struct{}
}

func (s *snapshotState) begin(limit int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if limit > 0 && s.inflight >= limit {
		return false
	}
	s.inflight++
	if s.inflight > s.maxConcurrent {
		s.maxConcurrent = s.inflight
	}
	return true
}

func (s *snapshotState) finish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight > 0 {
		s.inflight--
	}
}

func (s *snapshotState) wait(groupKey isr.GroupKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.waitingSet == nil {
		s.waitingSet = make(map[isr.GroupKey]struct{})
	}
	if _, exists := s.waitingSet[groupKey]; exists {
		return
	}
	s.waiting = append(s.waiting, groupKey)
	s.waitingSet[groupKey] = struct{}{}
}

func (s *snapshotState) popWaiter() (isr.GroupKey, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.waiting) == 0 {
		return "", false
	}
	groupKey := s.waiting[0]
	s.waiting = s.waiting[1:]
	delete(s.waitingSet, groupKey)
	return groupKey, true
}

func (s *snapshotState) maxObserved() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxConcurrent
}

func (s *snapshotState) waitingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.waiting)
}

func (r *runtime) queueSnapshot(groupKey isr.GroupKey) {
	r.queueSnapshotChunk(groupKey, 0)
}

func (r *runtime) queueSnapshotChunk(groupKey isr.GroupKey, bytes int64) {
	r.mu.RLock()
	g, ok := r.groups[groupKey]
	r.mu.RUnlock()
	if !ok {
		return
	}
	g.enqueueSnapshot(bytes)
	g.markSnapshot()
	r.enqueueScheduler(groupKey)
}

func (r *runtime) processSnapshot(groupKey isr.GroupKey) {
	r.mu.RLock()
	g, ok := r.groups[groupKey]
	r.mu.RUnlock()
	if !ok {
		return
	}

	if !r.snapshots.begin(r.cfg.Limits.MaxSnapshotInflight) {
		r.snapshots.wait(groupKey)
		return
	}

	bytes := g.drainSnapshotBytes()
	r.snapshotThrottle.Wait(bytes)
	r.completeSnapshot(groupKey)
}

func (r *runtime) maxSnapshotConcurrent() int {
	return r.snapshots.maxObserved()
}

func (r *runtime) completeSnapshot(groupKey isr.GroupKey) {
	r.snapshots.finish()
	if nextGroupKey, ok := r.snapshots.popWaiter(); ok {
		r.mu.RLock()
		g, exists := r.groups[nextGroupKey]
		r.mu.RUnlock()
		if exists {
			g.markSnapshot()
		}
		r.enqueueScheduler(nextGroupKey)
	}
}

func (r *runtime) queuedSnapshotGroups() int {
	return r.snapshots.waitingCount()
}
