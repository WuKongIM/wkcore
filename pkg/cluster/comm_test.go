package cluster_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"testing"
	"time"

	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	raftstorage "github.com/WuKongIM/WuKongIM/pkg/raftlog"
	metafsm "github.com/WuKongIM/WuKongIM/pkg/slot/fsm"
	metadb "github.com/WuKongIM/WuKongIM/pkg/slot/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	metastore "github.com/WuKongIM/WuKongIM/pkg/slot/proxy"
	"github.com/stretchr/testify/require"
)

// testNode bundles a cluster, store, and storage resources for testing.
type testNode struct {
	cluster            *raftcluster.Cluster
	store              *metastore.Store
	db                 *metadb.DB
	raftDB             *raftstorage.DB
	nodeID             multiraft.NodeID
	dir                string
	listenAddr         string
	nodes              []raftcluster.NodeConfig
	slots              []raftcluster.SlotConfig
	slotCount          int
	slotReplicaN       int
	controllerReplicaN int
	withController     bool
}

const (
	testClusterTickInterval   = 25 * time.Millisecond
	testClusterElectionTick   = 6
	testClusterHeartbeatTick  = 1
	testClusterDialTimeout    = 750 * time.Millisecond
	testClusterForwardTimeout = 750 * time.Millisecond
	testClusterPoolSize       = 1
	testLeaderPollInterval    = 50 * time.Millisecond
	testLeaderConfirmations   = 4
	testManagedSlotProbeWait  = 300 * time.Millisecond
)

func testClusterTimingConfig() raftcluster.Config {
	return raftcluster.Config{
		TickInterval:   testClusterTickInterval,
		ElectionTick:   testClusterElectionTick,
		HeartbeatTick:  testClusterHeartbeatTick,
		DialTimeout:    testClusterDialTimeout,
		ForwardTimeout: testClusterForwardTimeout,
		PoolSize:       testClusterPoolSize,
	}
}

func (n *testNode) stop() {
	if n == nil {
		return
	}
	if n.cluster != nil {
		n.cluster.Stop()
		n.cluster = nil
	}
	if n.raftDB != nil {
		_ = n.raftDB.Close()
		n.raftDB = nil
	}
	if n.db != nil {
		_ = n.db.Close()
		n.db = nil
	}
	n.store = nil
}

func newStartedTestNode(
	t testing.TB,
	dir string,
	nodeID multiraft.NodeID,
	listenAddr string,
	nodes []raftcluster.NodeConfig,
	slots []raftcluster.SlotConfig,
	slotCount int,
	slotReplicaN int,
	controllerReplicaN int,
	withController bool,
) *testNode {
	t.Helper()

	db, err := metadb.Open(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatalf("open metadb node %d: %v", nodeID, err)
	}
	raftDB, err := raftstorage.Open(filepath.Join(dir, "raft"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("open raftstorage node %d: %v", nodeID, err)
	}

	controllerMetaPath := ""
	controllerRaftPath := ""
	if withController {
		controllerMetaPath = filepath.Join(dir, "controller-meta")
		controllerRaftPath = filepath.Join(dir, "controller-raft")
	}

	cfg := raftcluster.Config{
		NodeID:             nodeID,
		ListenAddr:         listenAddr,
		SlotCount:          uint32(slotCount),
		SlotReplicaN:       slotReplicaN,
		ControllerReplicaN: controllerReplicaN,
		NewStorage: func(slotID multiraft.SlotID) (multiraft.Storage, error) {
			return raftDB.ForSlot(uint64(slotID)), nil
		},
		NewStateMachine:    metafsm.NewStateMachineFactory(db),
		Nodes:              append([]raftcluster.NodeConfig(nil), nodes...),
		Slots:              append([]raftcluster.SlotConfig(nil), slots...),
		ControllerMetaPath: controllerMetaPath,
		ControllerRaftPath: controllerRaftPath,
		TickInterval:       testClusterTickInterval,
		ElectionTick:       testClusterElectionTick,
		HeartbeatTick:      testClusterHeartbeatTick,
		DialTimeout:        testClusterDialTimeout,
		ForwardTimeout:     testClusterForwardTimeout,
		PoolSize:           testClusterPoolSize,
	}

	c, err := raftcluster.NewCluster(cfg)
	if err != nil {
		_ = raftDB.Close()
		_ = db.Close()
		t.Fatalf("NewCluster node %d: %v", nodeID, err)
	}
	if err := c.Start(); err != nil {
		_ = raftDB.Close()
		_ = db.Close()
		t.Fatalf("Start node %d: %v", nodeID, err)
	}

	return &testNode{
		cluster:            c,
		store:              metastore.New(c, db),
		db:                 db,
		raftDB:             raftDB,
		nodeID:             nodeID,
		dir:                dir,
		listenAddr:         listenAddr,
		nodes:              append([]raftcluster.NodeConfig(nil), nodes...),
		slots:              append([]raftcluster.SlotConfig(nil), slots...),
		slotCount:          slotCount,
		slotReplicaN:       slotReplicaN,
		controllerReplicaN: controllerReplicaN,
		withController:     withController,
	}
}

func startSingleNode(t testing.TB, slotCount int) *testNode {
	t.Helper()
	dir := t.TempDir()

	slots := make([]raftcluster.SlotConfig, slotCount)
	for i := range slotCount {
		slots[i] = raftcluster.SlotConfig{
			SlotID: multiraft.SlotID(i + 1),
			Peers:  []multiraft.NodeID{1},
		}
	}

	nodes := []raftcluster.NodeConfig{{NodeID: 1, Addr: "127.0.0.1:0"}}
	node := newStartedTestNode(t, dir, 1, "127.0.0.1:0", nodes, slots, slotCount, 1, 1, false)
	t.Cleanup(func() { stopNodes([]*testNode{node}) })
	return node
}

func startThreeNodes(t testing.TB, slotCount int) []*testNode {
	t.Helper()

	listeners := make([]net.Listener, 3)
	for i := range 3 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen %d: %v", i, err)
		}
		listeners[i] = ln
	}

	nodes := make([]raftcluster.NodeConfig, 3)
	for i := range 3 {
		nodes[i] = raftcluster.NodeConfig{
			NodeID: multiraft.NodeID(i + 1),
			Addr:   listeners[i].Addr().String(),
		}
		listeners[i].Close()
	}

	slots := make([]raftcluster.SlotConfig, slotCount)
	for i := range slotCount {
		slots[i] = raftcluster.SlotConfig{
			SlotID: multiraft.SlotID(i + 1),
			Peers:  []multiraft.NodeID{1, 2, 3},
		}
	}

	testNodes := make([]*testNode, 3)
	root := t.TempDir()
	for i := range 3 {
		dir := filepath.Join(root, fmt.Sprintf("n%d", i+1))
		testNodes[i] = newStartedTestNode(
			t,
			dir,
			multiraft.NodeID(i+1),
			nodes[i].Addr,
			nodes,
			slots,
			slotCount,
			3,
			3,
			false,
		)
	}
	t.Cleanup(func() { stopNodes(testNodes) })

	return testNodes
}

func startSingleNodeWithController(t testing.TB, slotCount int, legacySlotCount int) *testNode {
	t.Helper()
	dir := t.TempDir()

	slots := make([]raftcluster.SlotConfig, legacySlotCount)
	for i := range legacySlotCount {
		slots[i] = raftcluster.SlotConfig{
			SlotID: multiraft.SlotID(i + 1),
			Peers:  []multiraft.NodeID{1},
		}
	}

	nodes := []raftcluster.NodeConfig{{NodeID: 1, Addr: "127.0.0.1:0"}}
	node := newStartedTestNode(t, dir, 1, "127.0.0.1:0", nodes, slots, slotCount, 1, 1, true)
	t.Cleanup(func() { stopNodes([]*testNode{node}) })
	return node
}

func startThreeNodesWithController(t testing.TB, slotCount int, legacyReplicaN int) []*testNode {
	return startThreeNodesWithControllerWithSettle(t, slotCount, legacyReplicaN, true)
}

func startThreeNodesWithControllerWithSettle(t testing.TB, slotCount int, legacyReplicaN int, settle bool) []*testNode {
	t.Helper()

	listeners := make([]net.Listener, 3)
	for i := range 3 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen %d: %v", i, err)
		}
		listeners[i] = ln
	}

	nodes := make([]raftcluster.NodeConfig, 3)
	for i := range 3 {
		nodes[i] = raftcluster.NodeConfig{
			NodeID: multiraft.NodeID(i + 1),
			Addr:   listeners[i].Addr().String(),
		}
		listeners[i].Close()
	}

	testNodes := make([]*testNode, 3)
	root := t.TempDir()
	for i := range 3 {
		dir := filepath.Join(root, fmt.Sprintf("n%d", i+1))
		testNodes[i] = newStartedTestNode(
			t,
			dir,
			multiraft.NodeID(i+1),
			nodes[i].Addr,
			nodes,
			nil,
			slotCount,
			legacyReplicaN,
			3,
			true,
		)
	}
	t.Cleanup(func() { stopNodes(testNodes) })
	if settle {
		waitForManagedSlotsSettled(t, testNodes, slotCount)
	}
	return testNodes
}

func startFourNodesWithController(t testing.TB, slotCount int, replicaN int) []*testNode {
	t.Helper()

	listeners := make([]net.Listener, 4)
	for i := range 4 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen %d: %v", i, err)
		}
		listeners[i] = ln
	}

	nodes := make([]raftcluster.NodeConfig, 4)
	for i := range 4 {
		nodes[i] = raftcluster.NodeConfig{
			NodeID: multiraft.NodeID(i + 1),
			Addr:   listeners[i].Addr().String(),
		}
		listeners[i].Close()
	}

	testNodes := make([]*testNode, 4)
	root := t.TempDir()
	for i := range 4 {
		dir := filepath.Join(root, fmt.Sprintf("n%d", i+1))
		testNodes[i] = newStartedTestNode(
			t,
			dir,
			multiraft.NodeID(i+1),
			nodes[i].Addr,
			nodes,
			nil,
			slotCount,
			replicaN,
			3,
			true,
		)
	}
	t.Cleanup(func() { stopNodes(testNodes) })
	waitForManagedSlotsSettled(t, testNodes, slotCount)
	return testNodes
}

func startThreeOfFourNodesWithController(t testing.TB, slotCount int, replicaN int) []*testNode {
	t.Helper()

	listeners := make([]net.Listener, 4)
	for i := range 4 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen %d: %v", i, err)
		}
		listeners[i] = ln
	}

	nodes := make([]raftcluster.NodeConfig, 4)
	for i := range 4 {
		nodes[i] = raftcluster.NodeConfig{
			NodeID: multiraft.NodeID(i + 1),
			Addr:   listeners[i].Addr().String(),
		}
		listeners[i].Close()
	}

	testNodes := make([]*testNode, 4)
	root := t.TempDir()
	for i := range 4 {
		dir := filepath.Join(root, fmt.Sprintf("n%d", i+1))
		if i < 3 {
			testNodes[i] = newStartedTestNode(
				t,
				dir,
				multiraft.NodeID(i+1),
				nodes[i].Addr,
				nodes,
				nil,
				slotCount,
				replicaN,
				3,
				true,
			)
			continue
		}
		testNodes[i] = &testNode{
			nodeID:             multiraft.NodeID(i + 1),
			dir:                dir,
			listenAddr:         nodes[i].Addr,
			nodes:              append([]raftcluster.NodeConfig(nil), nodes...),
			slotCount:          slotCount,
			slotReplicaN:       replicaN,
			controllerReplicaN: 3,
			withController:     true,
		}
	}
	t.Cleanup(func() { stopNodes(testNodes) })
	waitForManagedSlotsSettled(t, testNodes[:3], slotCount)
	return testNodes
}

func startFourNodesWithInjectedRepairFailure(t testing.TB, slotCount int, replicaN int) []*testNode {
	t.Helper()
	nodes := startFourNodesWithController(t, slotCount, replicaN)
	waitForStableLeader(t, assignedNodesForSlot(t, nodes, 1), 1)
	restore := raftcluster.SetManagedSlotExecutionTestHook(func(slotID uint32, task controllermeta.ReconcileTask) error {
		if slotID == 1 && task.Kind == controllermeta.TaskKindRepair {
			return errors.New("injected repair failure")
		}
		return nil
	})
	t.Cleanup(restore)
	requireControllerCommand(t, nodes, func(cluster *raftcluster.Cluster) error {
		return cluster.MarkNodeDraining(context.Background(), 2)
	})
	require.Eventually(t, func() bool {
		controller, ok := currentControllerLeaderNode(nodes)
		if !ok {
			return false
		}
		task, err := controller.cluster.GetReconcileTask(context.Background(), 1)
		return err == nil && task.Attempt >= 1
	}, 20*time.Second, 100*time.Millisecond)
	return nodes
}

func startFourNodesWithPermanentRepairFailure(t testing.TB, slotCount int, replicaN int) []*testNode {
	t.Helper()
	return startFourNodesWithInjectedRepairFailure(t, slotCount, replicaN)
}

func stopNodes(nodes []*testNode) {
	stopped := false
	for _, node := range nodes {
		if node != nil && (node.cluster != nil || node.raftDB != nil || node.db != nil) {
			node.stop()
			stopped = true
		}
	}
	// Pebble-backed raft storage can still be finalizing the last batch commit
	// when the test temp dir cleanup runs. Give teardown a brief grace window.
	if stopped {
		time.Sleep(200 * time.Millisecond)
	}
}

func waitForControllerAssignments(t testing.TB, nodes []*testNode, slotCount int) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, node := range nodes {
			if node == nil || node.cluster == nil {
				continue
			}
			assignments, err := node.cluster.ListSlotAssignments(context.Background())
			if err == nil && len(assignments) == slotCount {
				return true
			}
		}
		return false
	}, 20*time.Second, 100*time.Millisecond)
}

func waitForManagedSlotsSettled(t testing.TB, nodes []*testNode, slotCount int) {
	t.Helper()
	waitForControllerAssignments(t, nodes, slotCount)

	require.Eventually(t, func() bool {
		assignments, ok := loadAssignments(nodes, slotCount)
		if !ok || len(assignments) != slotCount {
			return false
		}

		var probe *testNode
		for _, node := range nodes {
			if node != nil && node.cluster != nil {
				probe = node
				break
			}
		}
		if probe == nil {
			return false
		}

		for _, assignment := range assignments {
			slotNodes := make([]*testNode, 0, len(assignment.DesiredPeers))
			for _, peer := range assignment.DesiredPeers {
				idx := int(peer) - 1
				if idx < 0 || idx >= len(nodes) || nodes[idx] == nil || nodes[idx].cluster == nil {
					return false
				}
				slotNodes = append(slotNodes, nodes[idx])
			}
			if _, err := stableLeaderWithin(slotNodes, uint64(assignment.SlotID), testManagedSlotProbeWait); err != nil {
				return false
			}
			if _, err := probe.cluster.GetReconcileTask(context.Background(), assignment.SlotID); err == nil {
				return false
			} else if !errors.Is(err, controllermeta.ErrNotFound) {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond)
}

func waitForStableLeader(t testing.TB, testNodes []*testNode, slotID uint64) multiraft.NodeID {
	t.Helper()
	leaderID, err := stableLeaderWithin(testNodes, slotID, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return leaderID
}

func waitForAllStableLeaders(t testing.TB, testNodes []*testNode, slotCount int) map[uint64]multiraft.NodeID {
	t.Helper()
	type result struct {
		slotID   uint64
		leaderID multiraft.NodeID
	}
	results := make(chan result, slotCount)
	for g := 1; g <= slotCount; g++ {
		go func(gid uint64) {
			lid := waitForStableLeader(t, testNodes, gid)
			results <- result{gid, lid}
		}(uint64(g))
	}
	leaders := make(map[uint64]multiraft.NodeID, slotCount)
	for range slotCount {
		r := <-results
		leaders[r.slotID] = r.leaderID
	}
	return leaders
}

func restartNode(t testing.TB, nodes []*testNode, idx int) *testNode {
	t.Helper()

	old := nodes[idx]
	if old == nil {
		t.Fatalf("nodes[%d] is nil", idx)
	}

	nodeID := old.nodeID
	dir := old.dir
	listenAddr := old.listenAddr
	clusterNodes := append([]raftcluster.NodeConfig(nil), old.nodes...)
	slots := append([]raftcluster.SlotConfig(nil), old.slots...)
	slotCount := old.slotCount

	old.stop()

	restarted := newStartedTestNode(
		t,
		dir,
		nodeID,
		listenAddr,
		clusterNodes,
		slots,
		slotCount,
		old.slotReplicaN,
		old.controllerReplicaN,
		old.withController,
	)
	nodes[idx] = restarted
	return restarted
}

func waitForChannelVisibleOnNodes(t testing.TB, nodes []*testNode, channelID string, channelType int64) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		allVisible := true
		for _, node := range nodes {
			if node == nil || node.store == nil {
				continue
			}
			ch, err := node.store.GetChannel(context.Background(), channelID, channelType)
			if err != nil || ch.ChannelID != channelID {
				allVisible = false
				break
			}
		}
		if allVisible {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("channel %q type=%d not visible on all running nodes", channelID, channelType)
}

func stableLeaderWithin(testNodes []*testNode, slotID uint64, timeout time.Duration) (multiraft.NodeID, error) {
	deadline := time.Now().Add(timeout)
	var stableLeader multiraft.NodeID
	stableCount := 0

	for time.Now().Before(deadline) {
		var leaderID multiraft.NodeID
		allAgree := true
		for _, n := range testNodes {
			if n == nil || n.cluster == nil {
				continue
			}
			lid, err := n.cluster.LeaderOf(multiraft.SlotID(slotID))
			if err != nil {
				allAgree = false
				break
			}
			if leaderID == 0 {
				leaderID = lid
			} else if lid != leaderID {
				allAgree = false
				break
			}
		}
		if allAgree && leaderID != 0 {
			if leaderID == stableLeader {
				stableCount++
			} else {
				stableLeader = leaderID
				stableCount = 1
			}
			if stableCount >= testLeaderConfirmations {
				return stableLeader, nil
			}
		} else {
			stableCount = 0
			stableLeader = 0
		}
		time.Sleep(testLeaderPollInterval)
	}
	return 0, fmt.Errorf("no stable leader for slot %d", slotID)
}

func snapshotAssignments(t testing.TB, nodes []*testNode, slotCount int) []controllermeta.SlotAssignment {
	t.Helper()

	var snapshot []controllermeta.SlotAssignment
	require.Eventually(t, func() bool {
		assignments, ok := loadAssignments(nodes, slotCount)
		if ok {
			snapshot = assignments
			return true
		}
		return false
	}, 10*time.Second, 100*time.Millisecond)
	return snapshot
}

func loadAssignments(nodes []*testNode, slotCount int) ([]controllermeta.SlotAssignment, bool) {
	for _, node := range nodes {
		if node == nil || node.cluster == nil {
			continue
		}
		assignments, err := node.cluster.ListSlotAssignments(context.Background())
		if err == nil && len(assignments) == slotCount {
			return assignments, true
		}
	}
	return nil, false
}

func assignmentsContainPeer(assignments []controllermeta.SlotAssignment, peer uint64) bool {
	for _, assignment := range assignments {
		for _, candidate := range assignment.DesiredPeers {
			if candidate == peer {
				return true
			}
		}
	}
	return false
}

func slotAssignedToPeerAndController(assignments []controllermeta.SlotAssignment, peer, controllerLeader uint64) uint32 {
	for _, assignment := range assignments {
		hasPeer := false
		hasControllerLeader := false
		for _, candidate := range assignment.DesiredPeers {
			if candidate == peer {
				hasPeer = true
			}
			if candidate == controllerLeader {
				hasControllerLeader = true
			}
		}
		if hasPeer && hasControllerLeader {
			return assignment.SlotID
		}
	}
	return 0
}

func slotForControllerLeader(assignments []controllermeta.SlotAssignment, controllerLeader uint64) (uint32, uint64) {
	for _, assignment := range assignments {
		hasLeader := false
		var sourceNode uint64
		for _, candidate := range assignment.DesiredPeers {
			if candidate == controllerLeader {
				hasLeader = true
				continue
			}
			if sourceNode == 0 || candidate < sourceNode {
				sourceNode = candidate
			}
		}
		if hasLeader && sourceNode != 0 {
			return assignment.SlotID, sourceNode
		}
	}
	return 0, 0
}

type controllerProbeRequest struct {
	Kind string `json:"kind"`
}

type controllerProbeResponse struct {
	NotLeader bool   `json:"not_leader,omitempty"`
	LeaderID  uint64 `json:"leader_id,omitempty"`
}

func waitForControllerLeader(t testing.TB, nodes []*testNode) uint64 {
	t.Helper()

	payload, err := json.Marshal(controllerProbeRequest{Kind: "list_assignments"})
	require.NoError(t, err)

	var leaderID uint64
	require.Eventually(t, func() bool {
		for _, node := range nodes {
			if node == nil || node.cluster == nil {
				continue
			}
			controllerPeers := append([]raftcluster.NodeConfig(nil), node.nodes...)
			sort.Slice(controllerPeers, func(i, j int) bool {
				return controllerPeers[i].NodeID < controllerPeers[j].NodeID
			})
			if len(controllerPeers) > node.controllerReplicaN {
				controllerPeers = controllerPeers[:node.controllerReplicaN]
			}
			for _, peer := range controllerPeers {
				respBody, err := node.cluster.RPCService(
					context.Background(),
					peer.NodeID,
					multiraft.SlotID(^uint32(0)),
					14,
					payload,
				)
				if err != nil {
					continue
				}
				var resp controllerProbeResponse
				if err := json.Unmarshal(respBody, &resp); err != nil {
					continue
				}
				if resp.NotLeader && resp.LeaderID != 0 {
					leaderID = resp.LeaderID
					return true
				}
				leaderID = uint64(peer.NodeID)
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond)
	return leaderID
}

func currentControllerLeaderNode(nodes []*testNode) (*testNode, bool) {
	payload, err := json.Marshal(controllerProbeRequest{Kind: "list_assignments"})
	if err != nil {
		return nil, false
	}

	for _, node := range nodes {
		if node == nil || node.cluster == nil {
			continue
		}
		controllerPeers := append([]raftcluster.NodeConfig(nil), node.nodes...)
		sort.Slice(controllerPeers, func(i, j int) bool {
			return controllerPeers[i].NodeID < controllerPeers[j].NodeID
		})
		if len(controllerPeers) > node.controllerReplicaN {
			controllerPeers = controllerPeers[:node.controllerReplicaN]
		}
		for _, peer := range controllerPeers {
			respBody, err := node.cluster.RPCService(
				context.Background(),
				peer.NodeID,
				multiraft.SlotID(^uint32(0)),
				14,
				payload,
			)
			if err != nil {
				continue
			}
			var resp controllerProbeResponse
			if err := json.Unmarshal(respBody, &resp); err != nil {
				continue
			}
			leaderID := peer.NodeID
			if resp.NotLeader {
				if resp.LeaderID == 0 {
					continue
				}
				leaderID = multiraft.NodeID(resp.LeaderID)
			}
			idx := int(leaderID) - 1
			if idx >= 0 && idx < len(nodes) && nodes[idx] != nil && nodes[idx].cluster != nil {
				return nodes[idx], true
			}
		}
	}
	return nil, false
}

func controllerLeaderNode(t testing.TB, nodes []*testNode) *testNode {
	t.Helper()
	leaderID := waitForControllerLeader(t, nodes)
	idx := int(leaderID) - 1
	require.GreaterOrEqual(t, idx, 0)
	require.Less(t, idx, len(nodes))
	require.NotNil(t, nodes[idx])
	require.NotNil(t, nodes[idx].cluster)
	return nodes[idx]
}

func requireControllerCommand(t testing.TB, nodes []*testNode, fn func(*raftcluster.Cluster) error) {
	t.Helper()

	var lastErr error
	require.Eventually(t, func() bool {
		controller, ok := currentControllerLeaderNode(nodes)
		if !ok {
			lastErr = raftcluster.ErrNoLeader
			return false
		}
		lastErr = fn(controller.cluster)
		return lastErr == nil
	}, 15*time.Second, 200*time.Millisecond, "last controller command error: %v", lastErr)
}

func assignedNodesForSlot(t testing.TB, nodes []*testNode, slotID uint32) []*testNode {
	t.Helper()

	for _, node := range nodes {
		if node == nil || node.cluster == nil {
			continue
		}
		assignments, err := node.cluster.ListSlotAssignments(context.Background())
		if err != nil {
			continue
		}
		for _, assignment := range assignments {
			if assignment.SlotID != slotID {
				continue
			}
			assigned := make([]*testNode, 0, len(assignment.DesiredPeers))
			for _, peer := range assignment.DesiredPeers {
				idx := int(peer) - 1
				if idx >= 0 && idx < len(nodes) && nodes[idx] != nil {
					assigned = append(assigned, nodes[idx])
				}
			}
			require.NotEmpty(t, assigned)
			return assigned
		}
	}

	t.Fatalf("no assignment found for slot %d", slotID)
	return nil
}

func waitForLeader(t testing.TB, c *raftcluster.Cluster, slotID uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, err := c.LeaderOf(multiraft.SlotID(slotID))
		if err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no leader elected for slot %d", slotID)
}
