package multiisr

import (
	"sync"
	"time"
)

type snapshotState struct {
	mu            sync.Mutex
	inflight      int
	maxConcurrent int
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

func (s *snapshotState) maxObserved() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxConcurrent
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
		return
	}
	defer r.snapshots.finish()

	bytes := g.drainSnapshotBytes()
	if rate := r.cfg.Limits.MaxRecoveryBytesPerSecond; rate > 0 && bytes > 0 {
		delay := time.Duration(bytes*int64(time.Second)) / time.Duration(rate)
		if delay <= 0 {
			delay = time.Second
		}
		if r.advanceClock != nil {
			r.advanceClock(delay)
		}
	}
}

func (r *runtime) maxSnapshotConcurrent() int {
	return r.snapshots.maxObserved()
}
