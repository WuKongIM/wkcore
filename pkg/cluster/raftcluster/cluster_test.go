package raftcluster_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metafsm"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metastore"
	"github.com/WuKongIM/WuKongIM/pkg/storage/raftstorage"
)

// testNode bundles a cluster, store, and storage resources for testing.
type testNode struct {
	cluster    *raftcluster.Cluster
	store      *metastore.Store
	db         *metadb.DB
	raftDB     *raftstorage.DB
	nodeID     multiraft.NodeID
	dir        string
	listenAddr string
	nodes      []raftcluster.NodeConfig
	groups     []raftcluster.GroupConfig
	groupCount int
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

	cfg := raftcluster.Config{
		NodeID:     nodeID,
		ListenAddr: listenAddr,
		GroupCount: uint32(groupCount),
		NewStorage: func(groupID multiraft.GroupID) (multiraft.Storage, error) {
			return raftDB.ForGroup(uint64(groupID)), nil
		},
		NewStateMachine: metafsm.NewStateMachineFactory(db),
		Nodes:           append([]raftcluster.NodeConfig(nil), nodes...),
		Groups:          append([]raftcluster.GroupConfig(nil), groups...),
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
		cluster:    c,
		store:      metastore.New(c, db),
		db:         db,
		raftDB:     raftDB,
		nodeID:     nodeID,
		dir:        dir,
		listenAddr: listenAddr,
		nodes:      append([]raftcluster.NodeConfig(nil), nodes...),
		groups:     append([]raftcluster.GroupConfig(nil), groups...),
		groupCount: groupCount,
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
	return newStartedTestNode(t, dir, 1, "127.0.0.1:0", nodes, groups, groupCount)
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
		)
	}

	return testNodes
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

	restarted := newStartedTestNode(t, dir, nodeID, listenAddr, clusterNodes, groups, groupCount)
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
