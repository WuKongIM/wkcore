package multiisr

import (
	"sync"
	"time"
)

type snapshotState struct {
	mu            sync.Mutex
	inflight      int
	maxConcurrent int
	waiting       map[uint64]struct{}
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

func (s *snapshotState) wait(groupID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.waiting == nil {
		s.waiting = make(map[uint64]struct{})
	}
	s.waiting[groupID] = struct{}{}
}

func (s *snapshotState) popWaiter() (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for groupID := range s.waiting {
		delete(s.waiting, groupID)
		return groupID, true
	}
	return 0, false
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

func (r *runtime) queueSnapshot(groupID uint64) {
	r.queueSnapshotChunk(groupID, 0)
}

func (r *runtime) queueSnapshotChunk(groupID uint64, bytes int64) {
	r.mu.RLock()
	g, ok := r.groups[groupID]
	r.mu.RUnlock()
	if !ok {
		return
	}
	g.enqueueSnapshot(bytes)
	g.markSnapshot()
	r.scheduler.enqueue(groupID)
}

func (r *runtime) processSnapshot(groupID uint64) {
	r.mu.RLock()
	g, ok := r.groups[groupID]
	r.mu.RUnlock()
	if !ok {
		return
	}

	if !r.snapshots.begin(r.cfg.Limits.MaxSnapshotInflight) {
		r.snapshots.wait(groupID)
		return
	}

	bytes := g.drainSnapshotBytes()
	if r.snapshotRunner != nil && !r.snapshotRunner(groupID, bytes) {
		return
	}
	if rate := r.cfg.Limits.MaxRecoveryBytesPerSecond; rate > 0 && bytes > 0 {
		delay := time.Duration(bytes*int64(time.Second)) / time.Duration(rate)
		if delay <= 0 {
			delay = time.Second
		}
		if r.advanceClock != nil {
			r.advanceClock(delay)
		}
	}
	r.completeSnapshot(groupID)
}

func (r *runtime) maxSnapshotConcurrent() int {
	return r.snapshots.maxObserved()
}

func (r *runtime) completeSnapshot(groupID uint64) {
	r.snapshots.finish()
	if nextGroupID, ok := r.snapshots.popWaiter(); ok {
		r.scheduler.enqueue(nextGroupID)
	}
}

func (r *runtime) queuedSnapshotGroups() int {
	return r.snapshots.waitingCount()
}
