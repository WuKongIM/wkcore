package wkcluster

import (
	"context"
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
