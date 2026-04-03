package multiraft

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBenchmarkAppliedTrackerWaitForNode(t *testing.T) {
	tracker := newBenchmarkAppliedTracker()
	done := make(chan error, 1)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		done <- tracker.waitForNode(ctx, 1, 100, 7)
	}()

	time.Sleep(10 * time.Millisecond)
	tracker.markApplied(1, 100, 6)

	select {
	case err := <-done:
		t.Fatalf("waitForNode() returned early: %v", err)
	default:
	}

	tracker.markApplied(1, 100, 7)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waitForNode() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waitForNode() did not unblock")
	}
}

func TestBenchmarkAppliedTrackerWaitForAll(t *testing.T) {
	tracker := newBenchmarkAppliedTracker()
	done := make(chan error, 1)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		done <- tracker.waitForAll(ctx, []NodeID{1, 2, 3}, 101, 9)
	}()

	time.Sleep(10 * time.Millisecond)
	tracker.markApplied(1, 101, 9)
	tracker.markApplied(2, 101, 8)

	select {
	case err := <-done:
		t.Fatalf("waitForAll() returned early: %v", err)
	default:
	}

	tracker.markApplied(2, 101, 9)
	tracker.markApplied(3, 101, 9)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waitForAll() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waitForAll() did not unblock")
	}
}

func BenchmarkRuntimeTickFanout(b *testing.B) {
	for _, groupCount := range []int{64, 2048} {
		b.Run("groups="+strconv.Itoa(groupCount), func(b *testing.B) {
			rt := &Runtime{
				groups:    make(map[GroupID]*group, groupCount),
				scheduler: newScheduler(),
			}
			groupIDs := make([]GroupID, 0, groupCount)
			for i := 0; i < groupCount; i++ {
				id := GroupID(i + 1)
				rt.groups[id] = &group{id: id}
				groupIDs = append(groupIDs, id)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkTickFanout(rt)
				benchmarkDrainScheduler(rt.scheduler, len(groupIDs))
			}
		})
	}
}

func benchmarkTickFanout(rt *Runtime) {
	rt.mu.RLock()
	groups := make([]*group, 0, len(rt.groups))
	for _, g := range rt.groups {
		groups = append(groups, g)
	}
	rt.mu.RUnlock()

	for _, g := range groups {
		g.markTickPending()
		rt.scheduler.enqueue(g.id)
	}
}

func benchmarkDrainScheduler(s *scheduler, groupCount int) {
	for i := 0; i < groupCount; i++ {
		groupID := <-s.ch
		s.begin(groupID)
		_ = s.done(groupID)
	}
}

func BenchmarkThreeNodeMultiGroupProposalRoundTrip(b *testing.B) {
	for _, groupCount := range []int{8, 32} {
		b.Run("groups="+strconv.Itoa(groupCount), func(b *testing.B) {
			cluster := newAsyncTestCluster(b, []NodeID{1, 2, 3}, asyncNetworkConfig{
				MaxDelay: 2 * time.Millisecond,
				Seed:     int64(groupCount),
			})

			groupIDs := make([]GroupID, 0, groupCount)
			leaders := make(map[GroupID]NodeID, groupCount)
			for i := 0; i < groupCount; i++ {
				groupID := GroupID(10_000 + i)
				groupIDs = append(groupIDs, groupID)
				cluster.bootstrapGroup(b, groupID, []NodeID{1, 2, 3})
				cluster.waitForBootstrapApplied(b, groupID, 3)

				targetLeader := NodeID((i % 3) + 1)
				leaderID := cluster.waitForLeader(b, groupID)
				if leaderID != targetLeader {
					if err := cluster.runtime(leaderID).TransferLeadership(context.Background(), groupID, targetLeader); err != nil {
						b.Fatalf("TransferLeadership(group=%d) error = %v", groupID, err)
					}
					cluster.waitForSpecificLeader(b, groupID, targetLeader)
					leaderID = targetLeader
				}
				leaders[groupID] = leaderID
			}

			payloads := make([][]byte, len(groupIDs))
			for i, groupID := range groupIDs {
				payloads[i] = []byte("bench-group-" + strconv.FormatUint(uint64(groupID), 10))
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				groupIndex := i % len(groupIDs)
				groupID := groupIDs[groupIndex]
				leaderID := leaders[groupID]

				fut, err := cluster.runtime(leaderID).Propose(context.Background(), groupID, payloads[groupIndex])
				if err != nil {
					b.Fatalf("Propose(group=%d leader=%d) error = %v", groupID, leaderID, err)
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				res, err := fut.Wait(ctx)
				cancel()
				if err != nil {
					b.Fatalf("Wait(group=%d) error = %v", groupID, err)
				}

				cluster.waitForAllNodesAppliedIndex(b, groupID, res.Index)
			}
		})
	}
}

func BenchmarkThreeNodeMultiGroupProposalRoundTripNotified(b *testing.B) {
	for _, groupCount := range []int{8, 32} {
		b.Run("groups="+strconv.Itoa(groupCount), func(b *testing.B) {
			harness := newBenchmarkClusterHarness(b, groupCount, newBenchmarkAppliedTracker())

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				groupIndex := i % len(harness.groupIDs)
				if err := benchmarkProposeRoundTripNotified(harness, groupIndex); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkThreeNodeMultiGroupConcurrentProposalThroughput(b *testing.B) {
	for _, groupCount := range []int{8, 32} {
		b.Run("groups="+strconv.Itoa(groupCount), func(b *testing.B) {
			harness := newBenchmarkClusterHarness(b, groupCount, newBenchmarkAppliedTracker())
			workerCount := groupCount
			if workerCount > 12 {
				workerCount = 12
			}

			var (
				next   uint64
				failed atomic.Bool
				wg     sync.WaitGroup
			)
			errCh := make(chan error, 1)

			b.ReportAllocs()
			b.ResetTimer()
			for worker := 0; worker < workerCount; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					for {
						if failed.Load() {
							return
						}

						op := int(atomic.AddUint64(&next, 1)) - 1
						if op >= b.N {
							return
						}

						groupIndex := op % len(harness.groupIDs)
						if err := benchmarkProposeRoundTripNotified(harness, groupIndex); err != nil {
							if failed.CompareAndSwap(false, true) {
								errCh <- err
							}
							return
						}
					}
				}()
			}
			wg.Wait()
			b.StopTimer()

			select {
			case err := <-errCh:
				b.Fatal(err)
			default:
			}
		})
	}
}

type benchmarkClusterHarness struct {
	cluster  *testCluster
	nodeIDs  []NodeID
	groupIDs []GroupID
	leaders  []NodeID
	payloads [][]byte
	tracker  *benchmarkAppliedTracker
}

func newBenchmarkClusterHarness(b testing.TB, groupCount int, tracker *benchmarkAppliedTracker) *benchmarkClusterHarness {
	b.Helper()

	nodeIDs := []NodeID{1, 2, 3}
	cluster := newAsyncTestCluster(b, nodeIDs, asyncNetworkConfig{
		MaxDelay: 2 * time.Millisecond,
		Seed:     int64(groupCount),
	})

	groupIDs := make([]GroupID, 0, groupCount)
	leaders := make([]NodeID, 0, groupCount)
	payloads := make([][]byte, 0, groupCount)

	for i := 0; i < groupCount; i++ {
		groupID := GroupID(10_000 + i)
		groupIDs = append(groupIDs, groupID)

		if tracker != nil {
			cluster.bootstrapGroupWithAppliedTracker(b, tracker, groupID, nodeIDs)
		} else {
			cluster.bootstrapGroup(b, groupID, nodeIDs)
		}
		cluster.waitForBootstrapApplied(b, groupID, 3)

		targetLeader := nodeIDs[i%len(nodeIDs)]
		leaderID := cluster.waitForLeader(b, groupID)
		if leaderID != targetLeader {
			if err := cluster.runtime(leaderID).TransferLeadership(context.Background(), groupID, targetLeader); err != nil {
				b.Fatalf("TransferLeadership(group=%d) error = %v", groupID, err)
			}
			cluster.waitForSpecificLeader(b, groupID, targetLeader)
			leaderID = targetLeader
		}

		leaders = append(leaders, leaderID)
		payloads = append(payloads, []byte("bench-group-"+strconv.FormatUint(uint64(groupID), 10)))
	}

	return &benchmarkClusterHarness{
		cluster:  cluster,
		nodeIDs:  append([]NodeID(nil), nodeIDs...),
		groupIDs: groupIDs,
		leaders:  leaders,
		payloads: payloads,
		tracker:  tracker,
	}
}

func benchmarkProposeRoundTripNotified(harness *benchmarkClusterHarness, groupIndex int) error {
	groupID := harness.groupIDs[groupIndex]
	leaderID := harness.leaders[groupIndex]

	fut, err := harness.cluster.runtime(leaderID).Propose(context.Background(), groupID, harness.payloads[groupIndex])
	if err != nil {
		return fmt.Errorf("Propose(group=%d leader=%d): %w", groupID, leaderID, err)
	}

	res, err := fut.Wait(context.Background())
	if err != nil {
		return fmt.Errorf("Wait(group=%d): %w", groupID, err)
	}

	if err := harness.tracker.waitForAll(context.Background(), harness.nodeIDs, groupID, res.Index); err != nil {
		return fmt.Errorf("waitForAll(group=%d index=%d): %w", groupID, res.Index, err)
	}

	return nil
}

type benchmarkAppliedKey struct {
	nodeID  NodeID
	groupID GroupID
}

type benchmarkAppliedWaiter struct {
	index uint64
	ch    chan struct{}
}

type benchmarkAppliedTracker struct {
	mu      sync.Mutex
	applied map[benchmarkAppliedKey]uint64
	waiters map[benchmarkAppliedKey][]benchmarkAppliedWaiter
}

func newBenchmarkAppliedTracker() *benchmarkAppliedTracker {
	return &benchmarkAppliedTracker{
		applied: make(map[benchmarkAppliedKey]uint64),
		waiters: make(map[benchmarkAppliedKey][]benchmarkAppliedWaiter),
	}
}

func (t *benchmarkAppliedTracker) markApplied(nodeID NodeID, groupID GroupID, index uint64) {
	key := benchmarkAppliedKey{nodeID: nodeID, groupID: groupID}

	t.mu.Lock()
	if t.applied[key] >= index {
		t.mu.Unlock()
		return
	}
	t.applied[key] = index

	waiters := t.waiters[key]
	kept := waiters[:0]
	for _, waiter := range waiters {
		if waiter.index <= index {
			close(waiter.ch)
			continue
		}
		kept = append(kept, waiter)
	}
	if len(kept) == 0 {
		delete(t.waiters, key)
	} else {
		t.waiters[key] = kept
	}
	t.mu.Unlock()
}

func (t *benchmarkAppliedTracker) waitForNode(ctx context.Context, nodeID NodeID, groupID GroupID, index uint64) error {
	key := benchmarkAppliedKey{nodeID: nodeID, groupID: groupID}
	waiter := benchmarkAppliedWaiter{
		index: index,
		ch:    make(chan struct{}),
	}

	t.mu.Lock()
	if t.applied[key] >= index {
		t.mu.Unlock()
		return nil
	}
	t.waiters[key] = append(t.waiters[key], waiter)
	t.mu.Unlock()

	select {
	case <-waiter.ch:
		return nil
	case <-ctx.Done():
		t.mu.Lock()
		if t.applied[key] >= index {
			t.mu.Unlock()
			return nil
		}

		waiters := t.waiters[key]
		kept := waiters[:0]
		for _, candidate := range waiters {
			if candidate.ch == waiter.ch {
				continue
			}
			kept = append(kept, candidate)
		}
		if len(kept) == 0 {
			delete(t.waiters, key)
		} else {
			t.waiters[key] = kept
		}
		t.mu.Unlock()
		return ctx.Err()
	}
}

func (t *benchmarkAppliedTracker) waitForAll(ctx context.Context, nodeIDs []NodeID, groupID GroupID, index uint64) error {
	for _, nodeID := range nodeIDs {
		if err := t.waitForNode(ctx, nodeID, groupID, index); err != nil {
			return err
		}
	}
	return nil
}

type benchmarkNotifyingStorage struct {
	*internalFakeStorage
	tracker *benchmarkAppliedTracker
	nodeID  NodeID
	groupID GroupID
}

func newBenchmarkNotifyingStorage(tracker *benchmarkAppliedTracker, nodeID NodeID, groupID GroupID) *benchmarkNotifyingStorage {
	return &benchmarkNotifyingStorage{
		internalFakeStorage: &internalFakeStorage{},
		tracker:             tracker,
		nodeID:              nodeID,
		groupID:             groupID,
	}
}

func (s *benchmarkNotifyingStorage) MarkApplied(ctx context.Context, index uint64) error {
	if err := s.internalFakeStorage.MarkApplied(ctx, index); err != nil {
		return err
	}
	s.tracker.markApplied(s.nodeID, s.groupID, index)
	return nil
}

func (c *testCluster) bootstrapGroupWithAppliedTracker(t testing.TB, tracker *benchmarkAppliedTracker, groupID GroupID, voters []NodeID) {
	t.Helper()

	for _, nodeID := range voters {
		store := newBenchmarkNotifyingStorage(tracker, nodeID, groupID)
		fsm := &internalFakeStateMachine{}
		c.stores[nodeID][groupID] = store.internalFakeStorage
		c.fsms[nodeID][groupID] = fsm

		err := c.runtime(nodeID).BootstrapGroup(context.Background(), BootstrapGroupRequest{
			Group: GroupOptions{
				ID:           groupID,
				Storage:      store,
				StateMachine: fsm,
			},
			Voters: voters,
		})
		if err != nil {
			t.Fatalf("BootstrapGroup(node=%d) error = %v", nodeID, err)
		}
	}
}
