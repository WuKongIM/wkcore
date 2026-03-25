package multiraft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestThreeNodeClusterReplicatesProposalEndToEnd(t *testing.T) {
	cluster := newAsyncTestCluster(t, []NodeID{1, 2, 3}, asyncNetworkConfig{
		MaxDelay: 5 * time.Millisecond,
		Seed:     1,
	})
	groupID := GroupID(100)

	cluster.bootstrapGroup(t, groupID, []NodeID{1, 2, 3})
	cluster.waitForBootstrapApplied(t, groupID, 3)

	leaderID := cluster.waitForLeader(t, groupID)
	fut, err := cluster.runtime(leaderID).Propose(context.Background(), groupID, []byte("set a=1"))
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}

	res := waitForFutureResult(t, fut)
	if string(res.Data) != "ok:set a=1" {
		t.Fatalf("Wait().Data = %q", res.Data)
	}

	cluster.waitForAllApplied(t, groupID, []byte("set a=1"))
}

func TestThreeNodeClusterReplicatesMultipleProposalsInOrder(t *testing.T) {
	cluster := newAsyncTestCluster(t, []NodeID{1, 2, 3}, asyncNetworkConfig{
		MaxDelay: 5 * time.Millisecond,
		Seed:     2,
	})
	groupID := GroupID(101)

	cluster.bootstrapGroup(t, groupID, []NodeID{1, 2, 3})
	cluster.waitForBootstrapApplied(t, groupID, 3)

	leaderID := cluster.waitForLeader(t, groupID)
	commands := [][]byte{
		[]byte("set a=1"),
		[]byte("set b=2"),
		[]byte("set c=3"),
	}

	for _, command := range commands {
		fut, err := cluster.runtime(leaderID).Propose(context.Background(), groupID, command)
		if err != nil {
			t.Fatalf("Propose(%q) error = %v", command, err)
		}

		res := waitForFutureResult(t, fut)
		if string(res.Data) != "ok:"+string(command) {
			t.Fatalf("Wait(%q).Data = %q", command, res.Data)
		}
	}

	cluster.waitForAllAppliedSequence(t, groupID, commands)
}

func TestThreeNodeClusterTransfersLeadershipAndReplicatesAgain(t *testing.T) {
	cluster := newAsyncTestCluster(t, []NodeID{1, 2, 3}, asyncNetworkConfig{
		MaxDelay: 5 * time.Millisecond,
		Seed:     3,
	})
	groupID := GroupID(102)

	cluster.bootstrapGroup(t, groupID, []NodeID{1, 2, 3})
	cluster.waitForBootstrapApplied(t, groupID, 3)

	leaderID := cluster.waitForLeader(t, groupID)
	warmup, err := cluster.runtime(leaderID).Propose(context.Background(), groupID, []byte("warmup"))
	if err != nil {
		t.Fatalf("Propose(warmup) error = %v", err)
	}
	waitForFutureResult(t, warmup)
	cluster.waitForAllApplied(t, groupID, []byte("warmup"))

	targetLeader := cluster.pickFollower(leaderID)

	if err := cluster.runtime(leaderID).TransferLeadership(context.Background(), groupID, targetLeader); err != nil {
		t.Fatalf("TransferLeadership() error = %v", err)
	}

	cluster.waitForSpecificLeader(t, groupID, targetLeader)

	fut, err := cluster.runtime(targetLeader).Propose(context.Background(), groupID, []byte("set c=3"))
	if err != nil {
		t.Fatalf("Propose(newLeader=%d) error = %v", targetLeader, err)
	}

	res := waitForFutureResult(t, fut)
	if string(res.Data) != "ok:set c=3" {
		t.Fatalf("Wait().Data = %q", res.Data)
	}

	cluster.waitForAllApplied(t, groupID, []byte("set c=3"))
}

type testCluster struct {
	mu         sync.RWMutex
	network    *asyncTestNetwork
	runtimes   map[NodeID]*Runtime
	transports map[NodeID]*clusterTransport
	stores     map[NodeID]map[GroupID]*internalFakeStorage
	fsms       map[NodeID]map[GroupID]*internalFakeStateMachine
}

type asyncNetworkConfig struct {
	MaxDelay time.Duration
	Seed     int64
}

type asyncTestNetwork struct {
	maxDelay time.Duration
	stopCh   chan struct{}

	mu  sync.Mutex
	rng *rand.Rand
	err error
	wg  sync.WaitGroup

	cluster *testCluster
}

func newAsyncTestCluster(t *testing.T, nodeIDs []NodeID, cfg asyncNetworkConfig) *testCluster {
	t.Helper()

	network := newAsyncTestNetwork(cfg)
	cluster := &testCluster{
		network:    network,
		runtimes:   make(map[NodeID]*Runtime),
		transports: make(map[NodeID]*clusterTransport),
		stores:     make(map[NodeID]map[GroupID]*internalFakeStorage),
		fsms:       make(map[NodeID]map[GroupID]*internalFakeStateMachine),
	}
	network.cluster = cluster

	for _, nodeID := range nodeIDs {
		transport := &clusterTransport{network: network, from: nodeID}
		rt, err := New(Options{
			NodeID:       nodeID,
			TickInterval: 10 * time.Millisecond,
			Workers:      1,
			Transport:    transport,
			Raft: RaftOptions{
				ElectionTick:  20,
				HeartbeatTick: 1,
			},
		})
		if err != nil {
			t.Fatalf("New(node=%d) error = %v", nodeID, err)
		}

		cluster.runtimes[nodeID] = rt
		cluster.transports[nodeID] = transport
		cluster.stores[nodeID] = make(map[GroupID]*internalFakeStorage)
		cluster.fsms[nodeID] = make(map[GroupID]*internalFakeStateMachine)
	}

	t.Cleanup(func() {
		network.close()
		for _, rt := range cluster.runtimes {
			if err := rt.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
		}
	})

	return cluster
}

func newAsyncTestNetwork(cfg asyncNetworkConfig) *asyncTestNetwork {
	if cfg.Seed == 0 {
		cfg.Seed = 1
	}
	return &asyncTestNetwork{
		maxDelay: cfg.MaxDelay,
		stopCh:   make(chan struct{}),
		rng:      rand.New(rand.NewSource(cfg.Seed)),
	}
}

func (c *testCluster) runtime(nodeID NodeID) *Runtime {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.runtimes[nodeID]
}

func (c *testCluster) bootstrapGroup(t *testing.T, groupID GroupID, voters []NodeID) {
	t.Helper()

	for _, nodeID := range voters {
		store := &internalFakeStorage{}
		fsm := &internalFakeStateMachine{}
		c.stores[nodeID][groupID] = store
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

func waitForFutureResult(t *testing.T, fut Future) Result {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := fut.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	return res
}

func (c *testCluster) waitForLeader(t *testing.T, groupID GroupID) NodeID {
	t.Helper()

	return c.waitForStableLeader(t, groupID, 0)
}

func (c *testCluster) waitForSpecificLeader(t *testing.T, groupID GroupID, leaderID NodeID) {
	t.Helper()

	if got := c.waitForStableLeader(t, groupID, leaderID); got != leaderID {
		t.Fatalf("waitForStableLeader() = %d, want %d", got, leaderID)
	}
}

func (c *testCluster) waitForBootstrapApplied(t *testing.T, groupID GroupID, appliedIndex uint64) {
	t.Helper()

	c.waitForCondition(t, func() bool {
		for _, rt := range c.runtimes {
			st, err := rt.Status(groupID)
			if err != nil {
				return false
			}
			if st.AppliedIndex < appliedIndex || st.CommitIndex < appliedIndex {
				return false
			}
		}
		return true
	})
}

func (c *testCluster) pickFollower(leaderID NodeID) NodeID {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for nodeID := range c.runtimes {
		if nodeID != leaderID {
			return nodeID
		}
	}
	return 0
}

func (c *testCluster) waitForAllApplied(t *testing.T, groupID GroupID, data []byte) {
	t.Helper()

	c.waitForCondition(t, func() bool {
		for nodeID := range c.runtimes {
			fsm := c.fsms[nodeID][groupID]
			if fsm == nil {
				return false
			}
			fsm.mu.Lock()
			if len(fsm.applied) == 0 || string(fsm.applied[len(fsm.applied)-1]) != string(data) {
				fsm.mu.Unlock()
				return false
			}
			fsm.mu.Unlock()
		}
		return true
	})
}

func (c *testCluster) waitForAllAppliedSequence(t *testing.T, groupID GroupID, commands [][]byte) {
	t.Helper()

	c.waitForCondition(t, func() bool {
		for nodeID := range c.runtimes {
			fsm := c.fsms[nodeID][groupID]
			if fsm == nil {
				return false
			}
			fsm.mu.Lock()
			if len(fsm.applied) < len(commands) {
				fsm.mu.Unlock()
				return false
			}
			for i, command := range commands {
				if string(fsm.applied[i]) != string(command) {
					fsm.mu.Unlock()
					return false
				}
			}
			fsm.mu.Unlock()
		}
		return true
	})
}

func (c *testCluster) waitForStableLeader(t *testing.T, groupID GroupID, want NodeID) NodeID {
	t.Helper()

	const stableWindow = 100 * time.Millisecond

	var (
		lastLeader NodeID
		stableFrom time.Time
	)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.requireHealthyNetwork(t)

		leaderID, ok := c.currentLeader(groupID)
		if ok && (want == 0 || leaderID == want) {
			if leaderID != lastLeader {
				lastLeader = leaderID
				stableFrom = time.Now()
			}
			if time.Since(stableFrom) >= stableWindow {
				return leaderID
			}
		} else {
			lastLeader = 0
			stableFrom = time.Time{}
		}
		time.Sleep(10 * time.Millisecond)
	}

	c.requireHealthyNetwork(t)
	t.Fatal("cluster condition not satisfied before timeout")
	return 0
}

func (c *testCluster) currentLeader(groupID GroupID) (NodeID, bool) {
	var (
		leaderID    NodeID
		leaderCount int
	)
	for _, rt := range c.runtimes {
		st, err := rt.Status(groupID)
		if err != nil {
			return 0, false
		}
		if st.Role == RoleCandidate {
			return 0, false
		}
		if st.Role == RoleLeader {
			leaderCount++
			if leaderID == 0 {
				leaderID = st.NodeID
			}
			if st.NodeID != leaderID {
				return 0, false
			}
		}
		if st.LeaderID != 0 {
			if leaderID == 0 {
				leaderID = st.LeaderID
			}
			if st.LeaderID != leaderID {
				return 0, false
			}
		}
	}
	return leaderID, leaderCount == 1 && leaderID != 0
}

func (c *testCluster) waitForCondition(t *testing.T, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.requireHealthyNetwork(t)
		if fn() {
			c.requireHealthyNetwork(t)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	c.requireHealthyNetwork(t)
	t.Fatal("cluster condition not satisfied before timeout")
}

func (c *testCluster) requireHealthyNetwork(t *testing.T) {
	t.Helper()

	if err := c.network.firstError(); err != nil {
		t.Fatalf("network delivery error: %v", err)
	}
}

func (n *asyncTestNetwork) firstError() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.err
}

func (n *asyncTestNetwork) recordError(err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.err == nil {
		n.err = err
	}
}

func (n *asyncTestNetwork) randomDelay() time.Duration {
	if n.maxDelay <= 0 {
		return 0
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	return time.Duration(n.rng.Int63n(int64(n.maxDelay) + 1))
}

func (n *asyncTestNetwork) close() {
	close(n.stopCh)
	n.wg.Wait()
}

type clusterTransport struct {
	network *asyncTestNetwork
	from    NodeID
}

func (t *clusterTransport) Send(ctx context.Context, batch []Envelope) error {
	for _, env := range batch {
		t.network.wg.Add(1)
		env := Envelope{
			GroupID: env.GroupID,
			Message: cloneMessage(env.Message),
		}
		go func(env Envelope) {
			defer t.network.wg.Done()

			delay := t.network.randomDelay()
			if delay > 0 {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-t.network.stopCh:
					return
				case <-timer.C:
				}
			} else {
				select {
				case <-t.network.stopCh:
					return
				default:
				}
			}

			target := NodeID(env.Message.To)
			rt := t.network.cluster.runtime(target)
			if rt == nil {
				return
			}
			if err := rt.Step(context.Background(), env); err != nil &&
				!errors.Is(err, ErrRuntimeClosed) &&
				!errors.Is(err, ErrGroupClosed) &&
				!errors.Is(err, ErrGroupNotFound) {
				t.network.recordError(fmt.Errorf("deliver from=%d to=%d group=%d: %w", t.from, target, env.GroupID, err))
			}
		}(env)
	}
	return nil
}
