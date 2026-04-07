package isr

import "context"

type appendWaiter struct {
	target uint64
	result CommitResult
	ch     chan CommitResult
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

	base, err := r.log.Append(batch)
	if err != nil {
		r.mu.Unlock()
		return CommitResult{}, err
	}
	if err := r.log.Sync(); err != nil {
		r.mu.Unlock()
		return CommitResult{}, err
	}

	leo := base + uint64(len(batch))
	r.state.LEO = leo
	r.state.OffsetEpoch = offsetEpochForLEO(r.epochHistory, leo)
	r.setReplicaProgressLocked(r.localNode, leo)

	waiter := &appendWaiter{
		target: leo,
		result: CommitResult{
			BaseOffset:  base,
			RecordCount: len(batch),
		},
		ch: make(chan CommitResult, 1),
	}
	r.waiters = append(r.waiters, waiter)
	if err := r.advanceHWLocked(); err != nil {
		r.removeWaiterLocked(waiter)
		r.mu.Unlock()
		return CommitResult{}, err
	}
	ch := waiter.ch
	r.mu.Unlock()

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		r.mu.Lock()
		r.removeWaiterLocked(waiter)
		r.mu.Unlock()
		return CommitResult{}, ctx.Err()
	}
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
