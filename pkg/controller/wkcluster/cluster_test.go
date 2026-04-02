package wkcluster_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/controller/raftstore"
	"github.com/WuKongIM/WuKongIM/pkg/controller/wkcluster"
	"github.com/WuKongIM/WuKongIM/pkg/controller/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/controller/wkstore"
	"github.com/WuKongIM/WuKongIM/pkg/multiraft"
)

// testNode bundles a cluster, store, and storage resources for testing.
type testNode struct {
	cluster *wkcluster.Cluster
	store   *wkstore.Store
	db      *wkdb.DB
	raftDB  *raftstore.DB
	nodeID  multiraft.NodeID
}

func (n *testNode) stop() {
	n.cluster.Stop()
	if n.raftDB != nil {
		_ = n.raftDB.Close()
	}
	if n.db != nil {
		_ = n.db.Close()
	}
}

func startSingleNode(t testing.TB, groupCount int) *testNode {
	t.Helper()
	dir := t.TempDir()

	db, err := wkdb.Open(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatalf("open wkdb: %v", err)
	}
	raftDB, err := raftstore.Open(filepath.Join(dir, "raft"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("open raftstore: %v", err)
	}

	groups := make([]wkcluster.GroupConfig, groupCount)
	for i := range groupCount {
		groups[i] = wkcluster.GroupConfig{
			GroupID: multiraft.GroupID(i + 1),
			Peers:   []multiraft.NodeID{1},
		}
	}

	cfg := wkcluster.Config{
		NodeID:     1,
		ListenAddr: "127.0.0.1:0",
		GroupCount: uint32(groupCount),
		NewStorage: func(groupID multiraft.GroupID) (multiraft.Storage, error) {
			return raftDB.ForGroup(uint64(groupID)), nil
		},
		NewStateMachine: wkstore.NewStateMachineFactory(db),
		Nodes:           []wkcluster.NodeConfig{{NodeID: 1, Addr: "127.0.0.1:0"}},
		Groups:          groups,
	}

	c, err := wkcluster.NewCluster(cfg)
	if err != nil {
		_ = raftDB.Close()
		_ = db.Close()
		t.Fatalf("NewCluster: %v", err)
	}
	if err := c.Start(); err != nil {
		_ = raftDB.Close()
		_ = db.Close()
		t.Fatalf("Start: %v", err)
	}

	return &testNode{
		cluster: c,
		store:   wkstore.New(c, db),
		db:      db,
		raftDB:  raftDB,
		nodeID:  1,
	}
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

	nodes := make([]wkcluster.NodeConfig, 3)
	for i := range 3 {
		nodes[i] = wkcluster.NodeConfig{
			NodeID: multiraft.NodeID(i + 1),
			Addr:   listeners[i].Addr().String(),
		}
		listeners[i].Close()
	}

	groups := make([]wkcluster.GroupConfig, groupCount)
	for i := range groupCount {
		groups[i] = wkcluster.GroupConfig{
			GroupID: multiraft.GroupID(i + 1),
			Peers:   []multiraft.NodeID{1, 2, 3},
		}
	}

	testNodes := make([]*testNode, 3)
	for i := range 3 {
		dir := filepath.Join(t.TempDir(), fmt.Sprintf("n%d", i+1))

		db, err := wkdb.Open(filepath.Join(dir, "data"))
		if err != nil {
			t.Fatalf("open wkdb node %d: %v", i+1, err)
		}
		raftDB, err := raftstore.Open(filepath.Join(dir, "raft"))
		if err != nil {
			_ = db.Close()
			t.Fatalf("open raftstore node %d: %v", i+1, err)
		}

		cfg := wkcluster.Config{
			NodeID:     multiraft.NodeID(i + 1),
			ListenAddr: nodes[i].Addr,
			GroupCount: uint32(groupCount),
			NewStorage: func(groupID multiraft.GroupID) (multiraft.Storage, error) {
				return raftDB.ForGroup(uint64(groupID)), nil
			},
			NewStateMachine: wkstore.NewStateMachineFactory(db),
			Nodes:           nodes,
			Groups:          groups,
		}

		c, err := wkcluster.NewCluster(cfg)
		if err != nil {
			_ = raftDB.Close()
			_ = db.Close()
			t.Fatalf("NewCluster node %d: %v", i+1, err)
		}
		if err := c.Start(); err != nil {
			_ = raftDB.Close()
			_ = db.Close()
			t.Fatalf("Start node %d: %v", i+1, err)
		}

		testNodes[i] = &testNode{
			cluster: c,
			store:   wkstore.New(c, db),
			db:      db,
			raftDB:  raftDB,
			nodeID:  multiraft.NodeID(i + 1),
		}
	}

	return testNodes
}

func waitForLeader(t testing.TB, c *wkcluster.Cluster, groupID uint64) {
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

func waitForStableLeader(t testing.TB, testNodes []*testNode, groupID uint64) multiraft.NodeID {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var stableLeader multiraft.NodeID
	stableCount := 0
	requiredStable := 10

	for time.Now().Before(deadline) {
		var leaderID multiraft.NodeID
		allAgree := true
		for _, n := range testNodes {
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
				return stableLeader
			}
		} else {
			stableCount = 0
			stableLeader = 0
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("no stable leader for group %d", groupID)
	return 0
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

// ── Tests ───────────────────────────────────────────────────────────

func TestCluster_SingleNode_CreateAndGetChannel(t *testing.T) {
	n := startSingleNode(t, 1)
	defer n.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	waitForLeader(t, n.cluster, 1)

	if err := n.store.CreateChannel(ctx, "ch-1", 1); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}

	ch, err := n.store.GetChannel(ctx, "ch-1", 1)
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	if ch.ChannelID != "ch-1" || ch.ChannelType != 1 {
		t.Fatalf("unexpected channel: %+v", ch)
	}
}

func TestCluster_SingleNode_UpdateChannel(t *testing.T) {
	n := startSingleNode(t, 1)
	defer n.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	waitForLeader(t, n.cluster, 1)

	if err := n.store.CreateChannel(ctx, "ch-upd", 1); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	if err := n.store.UpdateChannel(ctx, "ch-upd", 1, 1); err != nil {
		t.Fatalf("UpdateChannel: %v", err)
	}

	ch, err := n.store.GetChannel(ctx, "ch-upd", 1)
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	if ch.Ban != 1 {
		t.Fatalf("expected Ban=1, got %d", ch.Ban)
	}
}

func TestCluster_SingleNode_DeleteChannel(t *testing.T) {
	n := startSingleNode(t, 1)
	defer n.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	waitForLeader(t, n.cluster, 1)

	if err := n.store.CreateChannel(ctx, "ch-del", 1); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	if err := n.store.DeleteChannel(ctx, "ch-del", 1); err != nil {
		t.Fatalf("DeleteChannel: %v", err)
	}

	_, err := n.store.GetChannel(ctx, "ch-del", 1)
	if err == nil {
		t.Fatal("expected error for deleted channel")
	}
}

func TestCluster_ThreeNode_ForwardToLeader(t *testing.T) {
	testNodes := startThreeNodes(t, 1)
	defer func() {
		for _, n := range testNodes {
			n.stop()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leaderID := waitForStableLeader(t, testNodes, 1)

	var leader, follower *testNode
	for _, n := range testNodes {
		if n.nodeID == leaderID {
			leader = n
		} else if follower == nil {
			follower = n
		}
	}
	if follower == nil || leader == nil {
		t.Fatal("could not identify leader/follower")
	}

	t.Logf("leader=node%d, follower=node%d", leader.nodeID, follower.nodeID)

	// Create channel on follower — should forward to leader
	if err := follower.store.CreateChannel(ctx, "forwarded-ch", 1); err != nil {
		t.Fatalf("CreateChannel on follower: %v", err)
	}

	time.Sleep(200 * time.Millisecond) // allow replication
	ch, err := leader.store.GetChannel(ctx, "forwarded-ch", 1)
	if err != nil {
		t.Fatalf("GetChannel on leader: %v", err)
	}
	if ch.ChannelID != "forwarded-ch" {
		t.Fatalf("unexpected: %+v", ch)
	}
}
