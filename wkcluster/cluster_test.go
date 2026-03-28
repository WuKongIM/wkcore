package wkcluster

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/WuKongIM/wraft/multiraft"
)

func TestCluster_SingleNode_CreateAndGetChannel(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NodeID:     1,
		ListenAddr: "127.0.0.1:0",
		GroupCount: 1,
		DataDir:    dir,
		Nodes:      []NodeConfig{{NodeID: 1, Addr: "127.0.0.1:0"}},
		Groups:     []GroupConfig{{GroupID: 1, Peers: []multiraft.NodeID{1}}},
	}

	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Wait for leader election
	waitForLeader(t, c, 1)

	// Create channel
	if err := c.CreateChannel(ctx, "ch-1", 1); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}

	// Read back
	ch, err := c.GetChannel(ctx, "ch-1", 1)
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	if ch.ChannelID != "ch-1" || ch.ChannelType != 1 {
		t.Fatalf("unexpected channel: %+v", ch)
	}
}

func TestCluster_SingleNode_UpdateChannel(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NodeID:     1,
		ListenAddr: "127.0.0.1:0",
		GroupCount: 1,
		DataDir:    dir,
		Nodes:      []NodeConfig{{NodeID: 1, Addr: "127.0.0.1:0"}},
		Groups:     []GroupConfig{{GroupID: 1, Peers: []multiraft.NodeID{1}}},
	}

	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	waitForLeader(t, c, 1)

	// Create then update
	if err := c.CreateChannel(ctx, "ch-upd", 1); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	if err := c.UpdateChannel(ctx, "ch-upd", 1, 1); err != nil {
		t.Fatalf("UpdateChannel: %v", err)
	}

	ch, err := c.GetChannel(ctx, "ch-upd", 1)
	if err != nil {
		t.Fatalf("GetChannel: %v", err)
	}
	if ch.Ban != 1 {
		t.Fatalf("expected Ban=1, got %d", ch.Ban)
	}
}

func TestCluster_SingleNode_DeleteChannel(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		NodeID:     1,
		ListenAddr: "127.0.0.1:0",
		GroupCount: 1,
		DataDir:    dir,
		Nodes:      []NodeConfig{{NodeID: 1, Addr: "127.0.0.1:0"}},
		Groups:     []GroupConfig{{GroupID: 1, Peers: []multiraft.NodeID{1}}},
	}

	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	waitForLeader(t, c, 1)

	// Create then delete
	if err := c.CreateChannel(ctx, "ch-del", 1); err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	if err := c.DeleteChannel(ctx, "ch-del", 1); err != nil {
		t.Fatalf("DeleteChannel: %v", err)
	}

	// Verify deleted
	_, err = c.GetChannel(ctx, "ch-del", 1)
	if err == nil {
		t.Fatal("expected error for deleted channel")
	}
}

func TestCluster_ThreeNode_ForwardToLeader(t *testing.T) {
	clusters := startThreeNodeCluster(t)
	defer func() {
		for _, c := range clusters {
			c.Stop()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Wait for all nodes to agree on the same leader
	leaderID := waitForStableLeader(t, clusters, 1)

	// Find leader and follower for group 1
	var leader, follower *Cluster
	for _, c := range clusters {
		if c.cfg.NodeID == leaderID {
			leader = c
		} else if follower == nil {
			follower = c
		}
	}
	if follower == nil || leader == nil {
		t.Fatal("could not identify leader/follower")
	}

	t.Logf("leader=node%d, follower=node%d", leader.cfg.NodeID, follower.cfg.NodeID)

	// Create channel on follower — should forward to leader
	if err := follower.CreateChannel(ctx, "forwarded-ch", 1); err != nil {
		t.Fatalf("CreateChannel on follower: %v", err)
	}

	// Verify on leader
	time.Sleep(200 * time.Millisecond) // allow replication
	ch, err := leader.GetChannel(ctx, "forwarded-ch", 1)
	if err != nil {
		t.Fatalf("GetChannel on leader: %v", err)
	}
	if ch.ChannelID != "forwarded-ch" {
		t.Fatalf("unexpected: %+v", ch)
	}
}

func startThreeNodeCluster(t testing.TB) []*Cluster {
	t.Helper()

	// Phase 1: Allocate ports by starting TCP listeners first
	listeners := make([]net.Listener, 3)
	for i := range 3 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen %d: %v", i, err)
		}
		listeners[i] = ln
	}

	nodes := make([]NodeConfig, 3)
	for i := range 3 {
		nodes[i] = NodeConfig{
			NodeID: multiraft.NodeID(i + 1),
			Addr:   listeners[i].Addr().String(),
		}
		listeners[i].Close() // Release port; Start() will rebind
	}

	// Phase 2: Start clusters with known addresses
	clusters := make([]*Cluster, 3)
	for i := range 3 {
		cfg := Config{
			NodeID:     multiraft.NodeID(i + 1),
			ListenAddr: nodes[i].Addr,
			GroupCount: 1,
			DataDir:    filepath.Join(t.TempDir(), fmt.Sprintf("n%d", i+1)),
			Nodes:      nodes,
			Groups: []GroupConfig{
				{GroupID: 1, Peers: []multiraft.NodeID{1, 2, 3}},
			},
		}
		c, err := NewCluster(cfg)
		if err != nil {
			t.Fatalf("NewCluster node %d: %v", i+1, err)
		}
		if err := c.Start(); err != nil {
			t.Fatalf("Start node %d: %v", i+1, err)
		}
		clusters[i] = c
	}

	return clusters
}

func waitForLeader(t testing.TB, c *Cluster, groupID uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err := c.runtime.Status(multiraft.GroupID(groupID))
		if err == nil && status.LeaderID != 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no leader elected for group %d", groupID)
}

// waitForStableLeader waits until all nodes agree on the same leader for a group
// and that leader remains consistent for multiple consecutive checks.
func waitForStableLeader(t testing.TB, clusters []*Cluster, groupID uint64) multiraft.NodeID {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var stableLeader multiraft.NodeID
	stableCount := 0
	requiredStable := 10 // 10 consecutive checks at 200ms = 2s of stability

	for time.Now().Before(deadline) {
		var leaderID multiraft.NodeID
		allAgree := true
		for _, c := range clusters {
			status, err := c.runtime.Status(multiraft.GroupID(groupID))
			if err != nil || status.LeaderID == 0 {
				allAgree = false
				break
			}
			if leaderID == 0 {
				leaderID = status.LeaderID
			} else if status.LeaderID != leaderID {
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
