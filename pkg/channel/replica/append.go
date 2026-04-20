package replica

import (
	"context"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/observability/sendtrace"
	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

func (r *replica) Append(ctx context.Context, batch []channel.Record) (channel.CommitResult, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return channel.CommitResult{}, channel.ErrNotLeader
	}
	if r.state.Role == channel.ReplicaRoleTombstoned {
		r.mu.Unlock()
		return channel.CommitResult{}, channel.ErrTombstoned
	}
	if r.state.Role == channel.ReplicaRoleFencedLeader {
		r.mu.Unlock()
		return channel.CommitResult{}, channel.ErrLeaseExpired
	}
	if r.state.Role != channel.ReplicaRoleLeader {
		r.mu.Unlock()
		return channel.CommitResult{}, channel.ErrNotLeader
	}
	if !r.state.CommitReady {
		r.mu.Unlock()
		return channel.CommitResult{}, channel.ErrNotReady
	}
	if !r.now().Before(r.meta.LeaseUntil) {
		r.state.Role = channel.ReplicaRoleFencedLeader
		r.publishStateLocked()
		r.mu.Unlock()
		return channel.CommitResult{}, channel.ErrLeaseExpired
	}
	if len(r.meta.ISR) < r.meta.MinISR {
		r.mu.Unlock()
		return channel.CommitResult{}, channel.ErrInsufficientISR
	}
	if len(batch) == 0 {
		res := channel.CommitResult{BaseOffset: r.state.LEO, NextCommitHW: r.state.HW}
		r.mu.Unlock()
		return res, nil
	}
	r.mu.Unlock()

	waiter := acquireAppendWaiter()
	waiter.result = channel.CommitResult{RecordCount: len(batch)}
	waiter.ch = ensureWaiterChannel(waiter.ch)

	req := acquireAppendRequest()
	req.ctx = ctx
	req.batch = batch
	req.byteCount = appendRequestBytes(batch)
	req.waiter = waiter
	req.enqueuedAt = r.now()
	req.waiter.enqueuedAt = req.enqueuedAt
	r.enqueueAppendRequest(req)

	select {
	case completion := <-req.waiter.ch:
		releaseAppendWaiter(req.waiter)
		releaseAppendRequest(req)
		return completion.result, completion.err
	case <-ctx.Done():
		r.appendMu.Lock()
		removed := r.removePendingAppendLocked(req)
		r.appendMu.Unlock()
		if removed {
			releaseAppendWaiter(req.waiter)
			releaseAppendRequest(req)
			return channel.CommitResult{}, ctx.Err()
		}

		r.mu.Lock()
		r.removeWaiterLocked(req.waiter)
		r.mu.Unlock()
		return channel.CommitResult{}, ctx.Err()
	}
}

func ensureWaiterChannel(ch chan appendCompletion) chan appendCompletion {
	if ch == nil {
		return make(chan appendCompletion, 1)
	}
	for {
		select {
		case <-ch:
		default:
			return ch
		}
	}
}

func (r *replica) enqueueAppendRequest(req *appendRequest) {
	r.appendMu.Lock()
	r.appendPending = append(r.appendPending, req)
	r.appendMu.Unlock()
	r.signalAppendCollector()
}

func (r *replica) startAppendCollector() {
	go func() {
		defer close(r.collectorDone)
		for {
			select {
			case <-r.appendSignal:
				for {
					batch := r.collectAppendBatch()
					if len(batch) == 0 {
						break
					}
					r.flushAppendBatch(batch)
				}
			case <-r.stopCh:
				return
			}
		}
	}()
}

func (r *replica) collectAppendBatch() []*appendRequest {
	waitLimit := r.appendGroupCommit.maxWait
	timedOut := waitLimit <= 0

	var (
		timer   *time.Timer
		timerCh <-chan time.Time
	)
	if waitLimit > 0 {
		timer = time.NewTimer(waitLimit)
		timerCh = timer.C
		defer timer.Stop()
	}

	for {
		r.appendMu.Lock()
		r.removeCanceledPendingLocked()
		if len(r.appendPending) == 0 {
			r.appendMu.Unlock()
			return nil
		}

		count, recordCount, byteCount := r.selectAppendBatchLocked()
		if timedOut || recordCount >= r.appendGroupCommit.maxRecords || byteCount >= r.appendGroupCommit.maxBytes {
			batch := make([]*appendRequest, count)
			copy(batch, r.appendPending[:count])
			copy(r.appendPending, r.appendPending[count:])
			r.appendPending = r.appendPending[:len(r.appendPending)-count]
			r.appendMu.Unlock()
			return batch
		}
		r.appendMu.Unlock()

		select {
		case <-r.appendSignal:
		case <-timerCh:
			timedOut = true
		case <-r.stopCh:
			return nil
		}
	}
}

func (r *replica) flushAppendBatch(batch []*appendRequest) {
	if len(batch) == 0 {
		return
	}

	active, mergedBuffer := r.buildActiveAppendBatch(batch)
	if mergedBuffer != nil {
		defer releaseMergedRecordBuffer(mergedBuffer)
	}
	if len(active) == 0 {
		return
	}

	r.mu.Lock()
	err := r.appendableLocked()
	r.mu.Unlock()
	if err != nil {
		r.completeAppendRequests(active, err)
		return
	}

	durableStartedAt := r.now()
	base, err := r.log.Append(mergedBuffer.records)
	if err != nil {
		r.completeAppendRequests(active, err)
		return
	}
	if err := r.log.Sync(); err != nil {
		r.completeAppendRequests(active, err)
		return
	}
	durableDoneAt := r.now()

	r.mu.Lock()
	if err := r.appendableLocked(); err != nil {
		r.mu.Unlock()
		r.completeAppendRequests(active, err)
		return
	}
	nextLEO := base
	for _, req := range active {
		target := nextLEO + uint64(len(req.batch))
		rangeStart := nextLEO + 1
		if req.ctx.Err() != nil {
			r.completeAppendWaiter(req.waiter, channel.CommitResult{}, req.ctx.Err())
			nextLEO = target
			continue
		}

		req.waiter.target = target
		req.waiter.rangeStart = rangeStart
		req.waiter.rangeEnd = target
		req.waiter.durableDoneAt = durableDoneAt
		req.waiter.result.BaseOffset = nextLEO
		req.waiter.result.RecordCount = len(req.batch)
		sendtrace.Record(sendtrace.Event{
			Stage:      sendtrace.StageReplicaLeaderQueueWait,
			At:         req.waiter.enqueuedAt,
			Duration:   sendtrace.Elapsed(req.waiter.enqueuedAt, durableStartedAt),
			NodeID:     uint64(r.localNode),
			ChannelKey: string(r.state.ChannelKey),
			RangeStart: rangeStart,
			RangeEnd:   target,
		})
		sendtrace.Record(sendtrace.Event{
			Stage:      sendtrace.StageReplicaLeaderLocalDurable,
			At:         durableStartedAt,
			Duration:   sendtrace.Elapsed(durableStartedAt, durableDoneAt),
			NodeID:     uint64(r.localNode),
			ChannelKey: string(r.state.ChannelKey),
			RangeStart: rangeStart,
			RangeEnd:   target,
		})
		r.waiters = append(r.waiters, req.waiter)
		nextLEO = target
	}
	r.state.LEO = nextLEO
	r.state.OffsetEpoch = offsetEpochForLEO(r.epochHistory, nextLEO)
	r.setReplicaProgressLocked(r.localNode, nextLEO)
	r.publishStateLocked()
	r.mu.Unlock()
	r.signalAdvanceHW()
}

func (r *replica) buildActiveAppendBatch(batch []*appendRequest) ([]*appendRequest, *pooledRecordBuffer) {
	active := make([]*appendRequest, 0, len(batch))
	recordCount := 0
	for _, req := range batch {
		if req.ctx.Err() != nil {
			r.completeAppendWaiter(req.waiter, channel.CommitResult{}, req.ctx.Err())
			continue
		}
		active = append(active, req)
		recordCount += len(req.batch)
	}
	if len(active) == 0 {
		return nil, nil
	}

	mergedBuffer := acquireMergedRecordBuffer()
	if cap(mergedBuffer.records) < recordCount {
		mergedBuffer.records = make([]channel.Record, 0, recordCount)
	}
	for _, req := range active {
		mergedBuffer.records = append(mergedBuffer.records, req.batch...)
	}
	return active, mergedBuffer
}

func (r *replica) appendableLocked() error {
	if r.closed {
		return channel.ErrNotLeader
	}
	if r.state.Role == channel.ReplicaRoleTombstoned {
		return channel.ErrTombstoned
	}
	if r.state.Role == channel.ReplicaRoleFencedLeader {
		return channel.ErrLeaseExpired
	}
	if r.state.Role != channel.ReplicaRoleLeader {
		return channel.ErrNotLeader
	}
	if !r.state.CommitReady {
		return channel.ErrNotReady
	}
	if !r.now().Before(r.meta.LeaseUntil) {
		r.state.Role = channel.ReplicaRoleFencedLeader
		r.publishStateLocked()
		return channel.ErrLeaseExpired
	}
	if len(r.meta.ISR) < r.meta.MinISR {
		return channel.ErrInsufficientISR
	}
	return nil
}

func (r *replica) selectAppendBatchLocked() (int, int, int) {
	var (
		count       int
		recordCount int
		byteCount   int
	)
	for i, req := range r.appendPending {
		nextRecordCount := recordCount + len(req.batch)
		nextByteCount := byteCount + req.byteCount
		if i > 0 && (nextRecordCount > r.appendGroupCommit.maxRecords || nextByteCount > r.appendGroupCommit.maxBytes) {
			break
		}
		count = i + 1
		recordCount = nextRecordCount
		byteCount = nextByteCount
	}
	if count == 0 && len(r.appendPending) > 0 {
		return 1, len(r.appendPending[0].batch), r.appendPending[0].byteCount
	}
	return count, recordCount, byteCount
}

func (r *replica) removeCanceledPendingLocked() {
	remaining := r.appendPending[:0]
	for _, req := range r.appendPending {
		if req.ctx.Err() == nil {
			remaining = append(remaining, req)
			continue
		}
		r.completeAppendWaiter(req.waiter, channel.CommitResult{}, req.ctx.Err())
	}
	r.appendPending = remaining
}

func (r *replica) removePendingAppendLocked(target *appendRequest) bool {
	for i, req := range r.appendPending {
		if req != target {
			continue
		}
		r.appendPending = append(r.appendPending[:i], r.appendPending[i+1:]...)
		return true
	}
	return false
}

func (r *replica) signalAppendCollector() {
	select {
	case r.appendSignal <- struct{}{}:
	default:
	}
}

func (r *replica) completeAppendRequests(requests []*appendRequest, err error) {
	for _, req := range requests {
		r.completeAppendWaiter(req.waiter, channel.CommitResult{}, err)
	}
}

func (r *replica) completeAppendWaiters(waiters []*appendWaiter, err error) {
	for _, waiter := range waiters {
		r.completeAppendWaiter(waiter, channel.CommitResult{}, err)
	}
}

func (r *replica) completeAppendWaiter(waiter *appendWaiter, result channel.CommitResult, err error) {
	if waiter == nil || waiter.ch == nil {
		return
	}
	select {
	case waiter.ch <- appendCompletion{result: result, err: err}:
	default:
	}
}

func appendRequestBytes(batch []channel.Record) int {
	total := 0
	for _, record := range batch {
		size := record.SizeBytes
		if size <= 0 {
			size = len(record.Payload)
		}
		total += size
	}
	return total
}

func (r *replica) removeWaiterLocked(target *appendWaiter) {
	for i, waiter := range r.waiters {
		if waiter != target {
			continue
		}
		r.waiters = append(r.waiters[:i], r.waiters[i+1:]...)
		return
	}
}

func (r *replica) failOutstandingAppendWorkLocked(err error) {
	pending := r.appendPending
	r.appendPending = nil
	waiters := r.waiters
	r.waiters = nil

	for _, req := range pending {
		r.completeAppendWaiter(req.waiter, channel.CommitResult{}, err)
	}
	for _, waiter := range waiters {
		r.completeAppendWaiter(waiter, channel.CommitResult{}, err)
	}
}
