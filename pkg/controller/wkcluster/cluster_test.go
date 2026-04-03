package wkcluster_test

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/controller/wkcluster"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metafsm"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metastore"
	"github.com/WuKongIM/WuKongIM/pkg/storage/raftstorage"
)

// testNode bundles a cluster, store, and storage resources for testing.
type testNode struct {
	cluster *wkcluster.Cluster
	store   *metastore.Store
	db      *metadb.DB
	raftDB  *raftstorage.DB
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

	db, err := metadb.Open(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatalf("open metadb: %v", err)
	}
	raftDB, err := raftstorage.Open(filepath.Join(dir, "raft"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("open raftstorage: %v", err)
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
		NewStateMachine: metafsm.NewStateMachineFactory(db),
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
		store:   metastore.New(c, db),
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

		db, err := metadb.Open(filepath.Join(dir, "data"))
		if err != nil {
			t.Fatalf("open metadb node %d: %v", i+1, err)
		}
		raftDB, err := raftstorage.Open(filepath.Join(dir, "raft"))
		if err != nil {
			_ = db.Close()
			t.Fatalf("open raftstorage node %d: %v", i+1, err)
		}

		cfg := wkcluster.Config{
			NodeID:     multiraft.NodeID(i + 1),
			ListenAddr: nodes[i].Addr,
			GroupCount: uint32(groupCount),
			NewStorage: func(groupID multiraft.GroupID) (multiraft.Storage, error) {
				return raftDB.ForGroup(uint64(groupID)), nil
			},
			NewStateMachine: metafsm.NewStateMachineFactory(db),
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
			store:   metastore.New(c, db),
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
