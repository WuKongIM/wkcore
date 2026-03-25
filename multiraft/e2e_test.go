package multiraft

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestThreeNodeClusterReplicatesProposalEndToEnd(t *testing.T) {
	cluster := newTestCluster(t, []NodeID{1, 2, 3})
	groupID := GroupID(100)

	cluster.bootstrapGroup(t, groupID, []NodeID{1, 2, 3})
	cluster.waitForBootstrapApplied(t, groupID, 3)
	cluster.campaign(t, 1, groupID)
	cluster.waitForSpecificLeader(t, groupID, 1)

	leaderID := NodeID(1)
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
	cluster := newTestCluster(t, []NodeID{1, 2, 3})
	groupID := GroupID(101)

	cluster.bootstrapGroup(t, groupID, []NodeID{1, 2, 3})
	cluster.waitForBootstrapApplied(t, groupID, 3)
	cluster.campaign(t, 1, groupID)
	cluster.waitForSpecificLeader(t, groupID, 1)

	leaderID := NodeID(1)
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
	cluster := newTestCluster(t, []NodeID{1, 2, 3})
	groupID := GroupID(102)

	cluster.bootstrapGroup(t, groupID, []NodeID{1, 2, 3})
	cluster.waitForBootstrapApplied(t, groupID, 3)
	cluster.campaign(t, 1, groupID)
	cluster.waitForSpecificLeader(t, groupID, 1)

	leaderID := NodeID(1)
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
	runtimes   map[NodeID]*Runtime
	transports map[NodeID]*clusterTransport
	stores     map[NodeID]map[GroupID]*internalFakeStorage
	fsms       map[NodeID]map[GroupID]*internalFakeStateMachine
}

func newTestCluster(t *testing.T, nodeIDs []NodeID) *testCluster {
	t.Helper()

	cluster := &testCluster{
		runtimes:   make(map[NodeID]*Runtime),
		transports: make(map[NodeID]*clusterTransport),
		stores:     make(map[NodeID]map[GroupID]*internalFakeStorage),
		fsms:       make(map[NodeID]map[GroupID]*internalFakeStateMachine),
	}

	for _, nodeID := range nodeIDs {
		transport := &clusterTransport{cluster: cluster, from: nodeID}
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
		for _, rt := range cluster.runtimes {
			if err := rt.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
		}
	})

	return cluster
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

func (c *testCluster) campaign(t *testing.T, nodeID NodeID, groupID GroupID) {
	t.Helper()

	rt := c.runtime(nodeID)
	if rt == nil {
		t.Fatalf("runtime(node=%d) not found", nodeID)
	}

	rt.mu.RLock()
	g := rt.groups[groupID]
	rt.mu.RUnlock()
	if g == nil {
		t.Fatalf("group(node=%d, group=%d) not found", nodeID, groupID)
	}

	g.enqueueControl(controlAction{kind: controlCampaign})
	rt.scheduler.enqueue(groupID)
}

func waitForFutureResult(t *testing.T, fut Future) Result {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err := fut.Wait(ctx)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	return res
}

func (c *testCluster) waitForLeader(t *testing.T, groupID GroupID) NodeID {
	t.Helper()

	var leader NodeID
	waitForClusterCondition(t, func() bool {
		count := 0
		var current NodeID
		for nodeID, rt := range c.runtimes {
			st, err := rt.Status(groupID)
			if err != nil {
				return false
			}
			if st.Role == RoleLeader {
				count++
				current = nodeID
			}
		}
		if count == 1 {
			leader = current
			return true
		}
		return false
	})
	return leader
}

func (c *testCluster) waitForSpecificLeader(t *testing.T, groupID GroupID, leaderID NodeID) {
	t.Helper()

	waitForClusterCondition(t, func() bool {
		for nodeID, rt := range c.runtimes {
			st, err := rt.Status(groupID)
			if err != nil {
				return false
			}
			if nodeID == leaderID && st.Role != RoleLeader {
				return false
			}
			if nodeID != leaderID && st.Role == RoleLeader {
				return false
			}
		}
		return true
	})
}

func (c *testCluster) waitForBootstrapApplied(t *testing.T, groupID GroupID, appliedIndex uint64) {
	t.Helper()

	waitForClusterCondition(t, func() bool {
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

	waitForClusterCondition(t, func() bool {
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

	waitForClusterCondition(t, func() bool {
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

func waitForClusterCondition(t *testing.T, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("cluster condition not satisfied before timeout")
}

type clusterTransport struct {
	cluster *testCluster
	from    NodeID
}

func (t *clusterTransport) Send(ctx context.Context, batch []Envelope) error {
	for _, env := range batch {
		target := NodeID(env.Message.To)
		rt := t.cluster.runtime(target)
		if rt == nil {
			continue
		}
		if err := rt.Step(ctx, env); err != nil {
			return err
		}
	}
	return nil
}
