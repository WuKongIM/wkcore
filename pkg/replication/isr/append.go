package isr

import (
	"context"
	"time"
)

type appendWaiter struct {
	target uint64
	result CommitResult
	ch     chan appendCompletion
}

func (r *replica) Append(ctx context.Context, batch []Record) (CommitResult, error) {
	r.mu.Lock()
	if r.state.Role == RoleTombstoned {
		r.mu.Unlock()
		return CommitResult{}, ErrTombstoned
	}
	if r.state.Role == RoleFencedLeader {
		r.mu.Unlock()
		return CommitResult{}, ErrLeaseExpired
	}
	if r.state.Role != RoleLeader {
		r.mu.Unlock()
		return CommitResult{}, ErrNotLeader
	}
	if !r.now().Before(r.meta.LeaseUntil) {
		r.state.Role = RoleFencedLeader
		r.mu.Unlock()
		return CommitResult{}, ErrLeaseExpired
	}
	if len(r.meta.ISR) < r.meta.MinISR {
		r.mu.Unlock()
		return CommitResult{}, ErrInsufficientISR
	}
	if len(batch) == 0 {
		res := CommitResult{BaseOffset: r.state.LEO, NextCommitHW: r.state.HW}
		r.mu.Unlock()
		return res, nil
	}
	r.mu.Unlock()

	req := &appendRequest{
		ctx:       ctx,
		batch:     batch,
		byteCount: appendRequestBytes(batch),
		waiter: &appendWaiter{
			result: CommitResult{RecordCount: len(batch)},
			ch:     make(chan appendCompletion, 1),
		},
	}

	if r.enqueueAppendRequest(req) {
		r.runAppendCollector()
	}

	select {
	case completion := <-req.waiter.ch:
		return completion.result, completion.err
	case <-ctx.Done():
		r.appendMu.Lock()
		removed := r.removePendingAppendLocked(req)
		r.appendMu.Unlock()
		if removed {
			return CommitResult{}, ctx.Err()
		}

		r.mu.Lock()
		r.removeWaiterLocked(req.waiter)
		r.mu.Unlock()
		return CommitResult{}, ctx.Err()
	}
}

func (r *replica) enqueueAppendRequest(req *appendRequest) bool {
	r.appendMu.Lock()
	defer r.appendMu.Unlock()

	r.appendPending = append(r.appendPending, req)
	if r.appendCollecting {
		r.signalAppendCollectorLocked()
		return false
	}

	r.appendCollecting = true
	return true
}

func (r *replica) runAppendCollector() {
	for {
		batch := r.collectAppendBatch()
		if len(batch) == 0 {
			return
		}
		r.flushAppendBatch(batch)
	}
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
			r.appendCollecting = false
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
		}
	}
}

func (r *replica) flushAppendBatch(batch []*appendRequest) {
	if len(batch) == 0 {
		return
	}

	r.appendMu.Lock()
	defer r.appendMu.Unlock()

	active, merged := r.buildActiveAppendBatch(batch)
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

	base, err := r.log.Append(merged)
	if err != nil {
		r.completeAppendRequests(active, err)
		return
	}
	if err := r.log.Sync(); err != nil {
		r.completeAppendRequests(active, err)
		return
	}

	r.mu.Lock()
	addedWaiters := make([]*appendWaiter, 0, len(active))
	nextLEO := base
	for _, req := range active {
		target := nextLEO + uint64(len(req.batch))
		if req.ctx.Err() != nil {
			r.completeAppendWaiter(req.waiter, CommitResult{}, req.ctx.Err())
			nextLEO = target
			continue
		}

		req.waiter.target = target
		req.waiter.result.BaseOffset = nextLEO
		req.waiter.result.RecordCount = len(req.batch)
		r.waiters = append(r.waiters, req.waiter)
		addedWaiters = append(addedWaiters, req.waiter)
		nextLEO = target
	}
	r.state.LEO = nextLEO
	r.state.OffsetEpoch = offsetEpochForLEO(r.epochHistory, nextLEO)
	r.setReplicaProgressLocked(r.localNode, nextLEO)
	r.mu.Unlock()

	if err := r.advanceHW(); err != nil {
		r.mu.Lock()
		for _, waiter := range addedWaiters {
			r.removeWaiterLocked(waiter)
		}
		r.mu.Unlock()
		r.completeAppendWaiters(addedWaiters, err)
		return
	}
}

func (r *replica) buildActiveAppendBatch(batch []*appendRequest) ([]*appendRequest, []Record) {
	active := make([]*appendRequest, 0, len(batch))
	recordCount := 0
	for _, req := range batch {
		if req.ctx.Err() != nil {
			r.completeAppendWaiter(req.waiter, CommitResult{}, req.ctx.Err())
			continue
		}
		active = append(active, req)
		recordCount += len(req.batch)
	}
	if len(active) == 0 {
		return nil, nil
	}

	merged := make([]Record, 0, recordCount)
	for _, req := range active {
		merged = append(merged, req.batch...)
	}
	return active, merged
}

func (r *replica) appendableLocked() error {
	if r.state.Role == RoleTombstoned {
		return ErrTombstoned
	}
	if r.state.Role == RoleFencedLeader {
		return ErrLeaseExpired
	}
	if r.state.Role != RoleLeader {
		return ErrNotLeader
	}
	if !r.now().Before(r.meta.LeaseUntil) {
		r.state.Role = RoleFencedLeader
		return ErrLeaseExpired
	}
	if len(r.meta.ISR) < r.meta.MinISR {
		return ErrInsufficientISR
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
		r.completeAppendWaiter(req.waiter, CommitResult{}, req.ctx.Err())
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

func (r *replica) signalAppendCollectorLocked() {
	select {
	case r.appendSignal <- struct{}{}:
	default:
	}
}

func (r *replica) completeAppendRequests(requests []*appendRequest, err error) {
	for _, req := range requests {
		r.completeAppendWaiter(req.waiter, CommitResult{}, err)
	}
}

func (r *replica) completeAppendWaiters(waiters []*appendWaiter, err error) {
	for _, waiter := range waiters {
		r.completeAppendWaiter(waiter, CommitResult{}, err)
	}
}

func (r *replica) completeAppendWaiter(waiter *appendWaiter, result CommitResult, err error) {
	waiter.ch <- appendCompletion{result: result, err: err}
	close(waiter.ch)
}

func appendRequestBytes(batch []Record) int {
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
