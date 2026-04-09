package raftcluster_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metafsm"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metastore"
	"github.com/WuKongIM/WuKongIM/pkg/storage/raftstorage"
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
	groups             []raftcluster.GroupConfig
	groupCount         int
	groupReplicaN      int
	controllerReplicaN int
	withController     bool
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
	groups []raftcluster.GroupConfig,
	groupCount int,
	groupReplicaN int,
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
		GroupCount:         uint32(groupCount),
		GroupReplicaN:      groupReplicaN,
		ControllerReplicaN: controllerReplicaN,
		NewStorage: func(groupID multiraft.GroupID) (multiraft.Storage, error) {
			return raftDB.ForGroup(uint64(groupID)), nil
		},
		NewStateMachine:    metafsm.NewStateMachineFactory(db),
		Nodes:              append([]raftcluster.NodeConfig(nil), nodes...),
		Groups:             append([]raftcluster.GroupConfig(nil), groups...),
		ControllerMetaPath: controllerMetaPath,
		ControllerRaftPath: controllerRaftPath,
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
		groups:             append([]raftcluster.GroupConfig(nil), groups...),
		groupCount:         groupCount,
		groupReplicaN:      groupReplicaN,
		controllerReplicaN: controllerReplicaN,
		withController:     withController,
	}
}

func startSingleNode(t testing.TB, groupCount int) *testNode {
	t.Helper()
	dir := t.TempDir()

	groups := make([]raftcluster.GroupConfig, groupCount)
	for i := range groupCount {
		groups[i] = raftcluster.GroupConfig{
			GroupID: multiraft.GroupID(i + 1),
			Peers:   []multiraft.NodeID{1},
		}
	}

	nodes := []raftcluster.NodeConfig{{NodeID: 1, Addr: "127.0.0.1:0"}}
	node := newStartedTestNode(t, dir, 1, "127.0.0.1:0", nodes, groups, groupCount, 1, 1, false)
	t.Cleanup(func() { stopNodes([]*testNode{node}) })
	return node
}

func startThreeNodes(t testing.TB, groupCount int) []*testNode {
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

	groups := make([]raftcluster.GroupConfig, groupCount)
	for i := range groupCount {
		groups[i] = raftcluster.GroupConfig{
			GroupID: multiraft.GroupID(i + 1),
			Peers:   []multiraft.NodeID{1, 2, 3},
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
			groups,
			groupCount,
			3,
			3,
			false,
		)
	}
	t.Cleanup(func() { stopNodes(testNodes) })

	return testNodes
}

func startSingleNodeWithController(t testing.TB, groupCount int, legacyGroupCount int) *testNode {
	t.Helper()
	dir := t.TempDir()

	groups := make([]raftcluster.GroupConfig, legacyGroupCount)
	for i := range legacyGroupCount {
		groups[i] = raftcluster.GroupConfig{
			GroupID: multiraft.GroupID(i + 1),
			Peers:   []multiraft.NodeID{1},
		}
	}

	nodes := []raftcluster.NodeConfig{{NodeID: 1, Addr: "127.0.0.1:0"}}
	node := newStartedTestNode(t, dir, 1, "127.0.0.1:0", nodes, groups, groupCount, 1, 1, true)
	t.Cleanup(func() { stopNodes([]*testNode{node}) })
	return node
}

func startThreeNodesWithController(t testing.TB, groupCount int, legacyReplicaN int) []*testNode {
	return startThreeNodesWithControllerWithSettle(t, groupCount, legacyReplicaN, true)
}

func startThreeNodesWithControllerWithSettle(t testing.TB, groupCount int, legacyReplicaN int, settle bool) []*testNode {
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
			groupCount,
			legacyReplicaN,
			3,
			true,
		)
	}
	t.Cleanup(func() { stopNodes(testNodes) })
	if settle {
		waitForManagedGroupsSettled(t, testNodes, groupCount)
	}
	return testNodes
}

func startFourNodesWithController(t testing.TB, groupCount int, replicaN int) []*testNode {
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
			groupCount,
			replicaN,
			3,
			true,
		)
	}
	t.Cleanup(func() { stopNodes(testNodes) })
	waitForManagedGroupsSettled(t, testNodes, groupCount)
	return testNodes
}

func startThreeOfFourNodesWithController(t testing.TB, groupCount int, replicaN int) []*testNode {
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
				groupCount,
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
			groupCount:         groupCount,
			groupReplicaN:      replicaN,
			controllerReplicaN: 3,
			withController:     true,
		}
	}
	t.Cleanup(func() { stopNodes(testNodes) })
	waitForManagedGroupsSettled(t, testNodes[:3], groupCount)
	return testNodes
}

func startFourNodesWithInjectedRepairFailure(t testing.TB, groupCount int, replicaN int) []*testNode {
	t.Helper()
	nodes := startFourNodesWithController(t, groupCount, replicaN)
	waitForStableLeader(t, assignedNodesForGroup(t, nodes, 1), 1)
	restore := raftcluster.SetManagedGroupExecutionTestHook(func(groupID uint32, task controllermeta.ReconcileTask) error {
		if groupID == 1 && task.Kind == controllermeta.TaskKindRepair {
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

func startFourNodesWithPermanentRepairFailure(t testing.TB, groupCount int, replicaN int) []*testNode {
	t.Helper()
	return startFourNodesWithInjectedRepairFailure(t, groupCount, replicaN)
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
		time.Sleep(3 * time.Second)
	}
}

func waitForControllerAssignments(t testing.TB, nodes []*testNode, groupCount int) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, node := range nodes {
			if node == nil || node.cluster == nil {
				continue
			}
			assignments, err := node.cluster.ListGroupAssignments(context.Background())
			if err == nil && len(assignments) == groupCount {
				return true
			}
		}
		return false
	}, 20*time.Second, 100*time.Millisecond)
}

func waitForManagedGroupsSettled(t testing.TB, nodes []*testNode, groupCount int) {
	t.Helper()
	waitForControllerAssignments(t, nodes, groupCount)

	require.Eventually(t, func() bool {
		assignments, ok := loadAssignments(nodes, groupCount)
		if !ok || len(assignments) != groupCount {
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
			groupNodes := make([]*testNode, 0, len(assignment.DesiredPeers))
			for _, peer := range assignment.DesiredPeers {
				idx := int(peer) - 1
				if idx < 0 || idx >= len(nodes) || nodes[idx] == nil || nodes[idx].cluster == nil {
					return false
				}
				groupNodes = append(groupNodes, nodes[idx])
			}
			if _, err := stableLeaderWithin(groupNodes, uint64(assignment.GroupID), 2*time.Second); err != nil {
				return false
			}
			if _, err := probe.cluster.GetReconcileTask(context.Background(), assignment.GroupID); err == nil {
				return false
			} else if !errors.Is(err, controllermeta.ErrNotFound) {
				return false
			}
		}
		return true
	}, 30*time.Second, 200*time.Millisecond)
}

func snapshotAssignments(t testing.TB, nodes []*testNode, groupCount int) []controllermeta.GroupAssignment {
	t.Helper()

	var snapshot []controllermeta.GroupAssignment
	require.Eventually(t, func() bool {
		assignments, ok := loadAssignments(nodes, groupCount)
		if ok {
			snapshot = assignments
			return true
		}
		return false
	}, 10*time.Second, 100*time.Millisecond)
	return snapshot
}

func loadAssignments(nodes []*testNode, groupCount int) ([]controllermeta.GroupAssignment, bool) {
	for _, node := range nodes {
		if node == nil || node.cluster == nil {
			continue
		}
		assignments, err := node.cluster.ListGroupAssignments(context.Background())
		if err == nil && len(assignments) == groupCount {
			return assignments, true
		}
	}
	return nil, false
}

func assignmentsContainPeer(assignments []controllermeta.GroupAssignment, peer uint64) bool {
	for _, assignment := range assignments {
		for _, candidate := range assignment.DesiredPeers {
			if candidate == peer {
				return true
			}
		}
	}
	return false
}

func groupAssignedToPeerAndController(assignments []controllermeta.GroupAssignment, peer, controllerLeader uint64) uint32 {
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
			return assignment.GroupID
		}
	}
	return 0
}

func groupForControllerLeader(assignments []controllermeta.GroupAssignment, controllerLeader uint64) (uint32, uint64) {
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
			return assignment.GroupID, sourceNode
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
					multiraft.GroupID(^uint32(0)),
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
				multiraft.GroupID(^uint32(0)),
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

func assignedNodesForGroup(t testing.TB, nodes []*testNode, groupID uint32) []*testNode {
	t.Helper()

	for _, node := range nodes {
		if node == nil || node.cluster == nil {
			continue
		}
		assignments, err := node.cluster.ListGroupAssignments(context.Background())
		if err != nil {
			continue
		}
		for _, assignment := range assignments {
			if assignment.GroupID != groupID {
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

	t.Fatalf("no assignment found for group %d", groupID)
	return nil
}

func waitForLeader(t testing.TB, c *raftcluster.Cluster, groupID uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, err := c.LeaderOf(multiraft.GroupID(groupID))
		if err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no leader elected for group %d", groupID)
}

func stableLeaderWithin(testNodes []*testNode, groupID uint64, timeout time.Duration) (multiraft.NodeID, error) {
	deadline := time.Now().Add(timeout)
	var stableLeader multiraft.NodeID
	stableCount := 0
	requiredStable := 10

	for time.Now().Before(deadline) {
		var leaderID multiraft.NodeID
		allAgree := true
		for _, n := range testNodes {
			if n == nil || n.cluster == nil {
				continue
			}
			lid, err := n.cluster.LeaderOf(multiraft.GroupID(groupID))
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
			if stableCount >= requiredStable {
				return stableLeader, nil
			}
		} else {
			stableCount = 0
			stableLeader = 0
		}
		time.Sleep(200 * time.Millisecond)
	}
	return 0, fmt.Errorf("no stable leader for group %d", groupID)
}

func waitForStableLeader(t testing.TB, testNodes []*testNode, groupID uint64) multiraft.NodeID {
	t.Helper()
	leaderID, err := stableLeaderWithin(testNodes, groupID, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return leaderID
}

func waitForAllStableLeaders(t testing.TB, testNodes []*testNode, groupCount int) map[uint64]multiraft.NodeID {
	t.Helper()
	type result struct {
		groupID  uint64
		leaderID multiraft.NodeID
	}
	results := make(chan result, groupCount)
	for g := 1; g <= groupCount; g++ {
		go func(gid uint64) {
			lid := waitForStableLeader(t, testNodes, gid)
			results <- result{gid, lid}
		}(uint64(g))
	}
	leaders := make(map[uint64]multiraft.NodeID, groupCount)
	for range groupCount {
		r := <-results
		leaders[r.groupID] = r.leaderID
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
	groups := append([]raftcluster.GroupConfig(nil), old.groups...)
	groupCount := old.groupCount

	old.stop()

	restarted := newStartedTestNode(
		t,
		dir,
		nodeID,
		listenAddr,
		clusterNodes,
		groups,
		groupCount,
		old.groupReplicaN,
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

func TestTestNodeRestartReopensClusterWithSameListenAddr(t *testing.T) {
	testNodes := startThreeNodes(t, 1)
	defer func() {
		for _, n := range testNodes {
			if n != nil {
				n.stop()
			}
		}
	}()

	old := testNodes[0]
	oldAddr := old.listenAddr
	oldDir := old.dir

	restarted := restartNode(t, testNodes, 0)

	if restarted.listenAddr != oldAddr {
		t.Fatalf("listenAddr = %q, want %q", restarted.listenAddr, oldAddr)
	}
	if restarted.dir != oldDir {
		t.Fatalf("dir = %q, want %q", restarted.dir, oldDir)
	}

	waitForStableLeader(t, testNodes, 1)
}

func TestThreeNodeClusterReelectsAfterLeaderRestart(t *testing.T) {
	testNodes := startThreeNodes(t, 1)
	defer func() {
		for _, n := range testNodes {
			if n != nil {
				n.stop()
			}
		}
	}()

	originalLeader := waitForStableLeader(t, testNodes, 1)
	leaderIdx := int(originalLeader - 1)

	testNodes[leaderIdx].stop()
	newLeader := waitForStableLeader(t, testNodes, 1)
	if newLeader == originalLeader {
		t.Fatalf("leader did not change after restarting node %d", originalLeader)
	}

	var follower *testNode
	for _, node := range testNodes {
		if node == nil || node.store == nil || node.cluster == nil || node.nodeID == newLeader {
			continue
		}
		follower = node
		break
	}
	if follower == nil {
		t.Fatal("no follower available after leader restart")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	duringFailoverID := fmt.Sprintf("reelect-during-failover-%d", time.Now().UnixNano())
	if err := follower.store.CreateChannel(ctx, duringFailoverID, 1); err != nil {
		t.Fatalf("CreateChannel during failover: %v", err)
	}
	waitForChannelVisibleOnNodes(t, testNodes, duringFailoverID, 1)

	restarted := restartNode(t, testNodes, leaderIdx)
	stableLeader := waitForStableLeader(t, testNodes, 1)
	if stableLeader == 0 {
		t.Fatal("missing stable leader after restarting old leader")
	}

	afterRestartID := fmt.Sprintf("reelect-after-restart-%d", time.Now().UnixNano())
	if err := restarted.store.CreateChannel(ctx, afterRestartID, 1); err != nil {
		t.Fatalf("CreateChannel via restarted node: %v", err)
	}
	waitForChannelVisibleOnNodes(t, testNodes, afterRestartID, 1)
}

func TestClusterReportsRuntimeViewsToController(t *testing.T) {
	nodes := startThreeNodesWithControllerWithSettle(t, 4, 3, false)
	defer stopNodes(nodes)

	require.Eventually(t, func() bool {
		views, err := nodes[0].cluster.ListObservedRuntimeViews(context.Background())
		return err == nil && len(views) == 4
	}, 40*time.Second, 100*time.Millisecond)
}

func TestClusterGroupIDsNoLongerDependOnStaticGroupConfig(t *testing.T) {
	node := startSingleNodeWithController(t, 8, 1)
	defer node.stop()

	require.Equal(t, []multiraft.GroupID{1, 2, 3, 4, 5, 6, 7, 8}, node.cluster.GroupIDs())
}

func TestClusterBootstrapsManagedGroupsFromControllerAssignments(t *testing.T) {
	nodes := startThreeNodesWithController(t, 4, 3)
	defer stopNodes(nodes)

	for groupID := 1; groupID <= 4; groupID++ {
		waitForStableLeader(t, nodes, uint64(groupID))
	}
}

func TestClusterContinuesServingWithOneReplicaNodeDown(t *testing.T) {
	nodes := startThreeNodesWithController(t, 1, 3)
	defer stopNodes(nodes)

	leaderIdx := int(waitForStableLeader(t, nodes, 1) - 1)
	nodes[leaderIdx].stop()

	require.Eventually(t, func() bool {
		_, err := nodes[(leaderIdx+1)%3].cluster.LeaderOf(1)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)
}

func TestClusterListGroupAssignmentsReflectsControllerState(t *testing.T) {
	nodes := startFourNodesWithController(t, 2, 3)
	defer stopNodes(nodes)

	assignments, err := nodes[0].cluster.ListGroupAssignments(context.Background())
	require.NoError(t, err)
	require.Len(t, assignments, 2)
	require.Len(t, assignments[0].DesiredPeers, 3)
}

func TestClusterTransferGroupLeaderDelegatesToManagedGroup(t *testing.T) {
	nodes := startThreeNodesWithController(t, 1, 3)
	defer stopNodes(nodes)

	currentLeader := waitForStableLeader(t, nodes, 1)
	target := multiraft.NodeID(1)
	if currentLeader == target {
		target = 2
	}

	require.NoError(t, nodes[0].cluster.TransferGroupLeader(context.Background(), 1, target))
	require.Eventually(t, func() bool {
		leader, err := nodes[0].cluster.LeaderOf(1)
		return err == nil && leader == target
	}, 10*time.Second, 100*time.Millisecond)
}

func TestClusterMarkNodeDrainingMovesAssignmentsAway(t *testing.T) {
	nodes := startFourNodesWithController(t, 2, 3)
	defer stopNodes(nodes)

	require.NoError(t, nodes[0].cluster.MarkNodeDraining(context.Background(), 1))
	require.Eventually(t, func() bool {
		assignments, err := nodes[0].cluster.ListGroupAssignments(context.Background())
		if err != nil {
			return false
		}
		for _, assignment := range assignments {
			for _, peer := range assignment.DesiredPeers {
				if peer == 1 {
					return false
				}
			}
		}
		return true
	}, 20*time.Second, 200*time.Millisecond)
}

func TestClusterForceReconcileRetriesFailedRepair(t *testing.T) {
	nodes := startFourNodesWithInjectedRepairFailure(t, 1, 3)
	defer stopNodes(nodes)

	requireControllerCommand(t, nodes, func(cluster *raftcluster.Cluster) error {
		return cluster.ForceReconcile(context.Background(), 1)
	})
	require.Eventually(t, func() bool {
		controller, ok := currentControllerLeaderNode(nodes)
		if !ok {
			return false
		}
		task, err := controller.cluster.GetReconcileTask(context.Background(), 1)
		return err == nil && task.Attempt >= 2
	}, 20*time.Second, 100*time.Millisecond)
}

func TestClusterSurfacesFailedRepairAfterRetryExhaustion(t *testing.T) {
	nodes := startFourNodesWithPermanentRepairFailure(t, 1, 3)
	defer stopNodes(nodes)

	require.Eventually(t, func() bool {
		controller, ok := currentControllerLeaderNode(nodes)
		if !ok {
			return false
		}
		task, err := controller.cluster.GetReconcileTask(context.Background(), 1)
		return err == nil && task.Status == raftcluster.TaskStatusFailed && task.Attempt == 3
	}, 30*time.Second, 200*time.Millisecond)
}

func TestClusterRecoverGroupReturnsManualRecoveryErrorWhenQuorumLost(t *testing.T) {
	nodes := startThreeNodesWithController(t, 1, 3)
	defer stopNodes(nodes)

	waitForStableLeader(t, nodes, 1)
	nodes[1].stop()
	nodes[2].stop()

	err := nodes[0].cluster.RecoverGroup(context.Background(), 1, raftcluster.RecoverStrategyLatestLiveReplica)
	require.ErrorIs(t, err, raftcluster.ErrManualRecoveryRequired)
}

func TestClusterRebalancesAfterNewWorkerNodeJoins(t *testing.T) {
	nodes := startThreeOfFourNodesWithController(t, 2, 3)
	defer stopNodes(nodes)

	assignments := snapshotAssignments(t, nodes[:3], 2)
	require.False(t, assignmentsContainPeer(assignments, 4))

	nodes[3] = restartNode(t, nodes, 3)

	require.Eventually(t, func() bool {
		assignments, ok := loadAssignments(nodes, 2)
		return ok && assignmentsContainPeer(assignments, 4)
	}, 20*time.Second, 200*time.Millisecond)
}

func TestClusterRebalancesAfterRecoveredNodeReturns(t *testing.T) {
	nodes := startFourNodesWithController(t, 2, 3)
	defer stopNodes(nodes)

	require.Eventually(t, func() bool {
		assignments, ok := loadAssignments(nodes, 2)
		return ok && assignmentsContainPeer(assignments, 4)
	}, 20*time.Second, 200*time.Millisecond)

	requireControllerCommand(t, nodes, func(cluster *raftcluster.Cluster) error {
		return cluster.MarkNodeDraining(context.Background(), 4)
	})

	require.Eventually(t, func() bool {
		assignments, ok := loadAssignments(nodes, 2)
		return ok && !assignmentsContainPeer(assignments, 4)
	}, 20*time.Second, 200*time.Millisecond)

	requireControllerCommand(t, nodes, func(cluster *raftcluster.Cluster) error {
		return cluster.ResumeNode(context.Background(), 4)
	})

	require.Eventually(t, func() bool {
		assignments, ok := loadAssignments(nodes, 2)
		return ok && assignmentsContainPeer(assignments, 4)
	}, 20*time.Second, 200*time.Millisecond)
}

func TestClusterControllerLeaderFailoverResumesInFlightRepair(t *testing.T) {
	nodes := startFourNodesWithController(t, 2, 3)
	defer stopNodes(nodes)

	controllerLeader := waitForControllerLeader(t, nodes)
	assignments := snapshotAssignments(t, nodes, 2)
	groupID, sourceNode := groupForControllerLeader(assignments, controllerLeader)
	require.NotZero(t, groupID)
	require.NotZero(t, sourceNode)

	var failRepair atomic.Bool
	failRepair.Store(true)
	var repairExecCount atomic.Int32
	restore := raftcluster.SetManagedGroupExecutionTestHook(func(taskGroupID uint32, task controllermeta.ReconcileTask) error {
		if failRepair.Load() && taskGroupID == groupID && task.Kind == controllermeta.TaskKindRepair {
			repairExecCount.Add(1)
			return errors.New("injected repair failure")
		}
		return nil
	})
	defer restore()

	requireControllerCommand(t, nodes, func(cluster *raftcluster.Cluster) error {
		return cluster.MarkNodeDraining(context.Background(), sourceNode)
	})
	require.Eventually(t, func() bool {
		controller, ok := currentControllerLeaderNode(nodes)
		if !ok {
			return false
		}
		if err := controller.cluster.ForceReconcile(context.Background(), groupID); err != nil {
			return false
		}
		_, err := controller.cluster.GetReconcileTask(context.Background(), groupID)
		return err == nil
	}, 10*time.Second, 200*time.Millisecond)
	require.Eventually(t, func() bool {
		return repairExecCount.Load() >= 1
	}, 30*time.Second, 200*time.Millisecond)

	nodes[int(controllerLeader-1)].stop()
	failRepair.Store(false)

	require.Eventually(t, func() bool {
		assignments, ok := loadAssignments(nodes, 2)
		if !ok {
			return false
		}
		for _, assignment := range assignments {
			if assignment.GroupID == groupID {
				for _, peer := range assignment.DesiredPeers {
					if peer == sourceNode {
						return false
					}
				}
				return true
			}
		}
		return false
	}, 20*time.Second, 200*time.Millisecond)
}
