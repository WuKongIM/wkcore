package node

import (
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
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
	waiting       []isr.ChannelKey
	waitingSet    map[isr.ChannelKey]struct{}
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

func (s *snapshotState) wait(channelKey isr.ChannelKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.waitingSet == nil {
		s.waitingSet = make(map[isr.ChannelKey]struct{})
	}
	if _, exists := s.waitingSet[channelKey]; exists {
		return
	}
	s.waiting = append(s.waiting, channelKey)
	s.waitingSet[channelKey] = struct{}{}
}

func (s *snapshotState) popWaiter() (isr.ChannelKey, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.waiting) == 0 {
		return "", false
	}
	channelKey := s.waiting[0]
	s.waiting = s.waiting[1:]
	delete(s.waitingSet, channelKey)
	return channelKey, true
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

func (r *runtime) queueSnapshot(channelKey isr.ChannelKey) {
	r.queueSnapshotChunk(channelKey, 0)
}

func (r *runtime) queueSnapshotChunk(channelKey isr.ChannelKey, bytes int64) {
	r.mu.RLock()
	g, ok := r.groups[channelKey]
	r.mu.RUnlock()
	if !ok {
		return
	}
	g.enqueueSnapshot(bytes)
	g.markSnapshot()
	r.enqueueScheduler(channelKey)
}

func (r *runtime) processSnapshot(channelKey isr.ChannelKey) {
	r.mu.RLock()
	g, ok := r.groups[channelKey]
	r.mu.RUnlock()
	if !ok {
		return
	}

	if !r.snapshots.begin(r.cfg.Limits.MaxSnapshotInflight) {
		r.snapshots.wait(channelKey)
		return
	}

	bytes := g.drainSnapshotBytes()
	r.snapshotThrottle.Wait(bytes)
	r.completeSnapshot(channelKey)
}

func (r *runtime) maxSnapshotConcurrent() int {
	return r.snapshots.maxObserved()
}

func (r *runtime) completeSnapshot(channelKey isr.ChannelKey) {
	r.snapshots.finish()
	if nextChannelKey, ok := r.snapshots.popWaiter(); ok {
		r.mu.RLock()
		g, exists := r.groups[nextChannelKey]
		r.mu.RUnlock()
		if exists {
			g.markSnapshot()
		}
		r.enqueueScheduler(nextChannelKey)
	}
}

func (r *runtime) queuedSnapshotGroups() int {
	return r.snapshots.waitingCount()
}
