package cluster

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestControllerHostStartElectsSingleLocalPeer(t *testing.T) {
	cfg := validTestConfig()
	cfg.ControllerReplicaN = 1
	cfg.Nodes = []NodeConfig{{NodeID: cfg.NodeID, Addr: "127.0.0.1:0"}}
	cfg.ControllerMetaPath = filepath.Join(t.TempDir(), "controller-meta")
	cfg.ControllerRaftPath = filepath.Join(t.TempDir(), "controller-raft")

	discovery := NewStaticDiscovery(cfg.Nodes)
	layer := newTransportLayer(cfg, discovery, nil)
	requireNoErr(t, layer.Start(
		"127.0.0.1:0",
		func([]byte) {},
		func(context.Context, []byte) ([]byte, error) { return nil, nil },
		func(context.Context, []byte) ([]byte, error) { return nil, nil },
		func(context.Context, []byte) ([]byte, error) { return nil, nil },
	))
	t.Cleanup(layer.Stop)

	cfg.Nodes[0].Addr = layer.server.Listener().Addr().String()
	host, err := newControllerHost(cfg, layer)
	if err != nil {
		t.Fatalf("newControllerHost() error = %v", err)
	}
	requireNoErr(t, host.Start(context.Background()))
	t.Cleanup(host.Stop)

	deadline := time.Now().Add(2 * time.Second)
	for host.LeaderID() != cfg.NodeID && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if host.LeaderID() != cfg.NodeID {
		t.Fatalf("controllerHost.LeaderID() = %d, want %d", host.LeaderID(), cfg.NodeID)
	}
	if !host.IsLeader(cfg.NodeID) {
		t.Fatal("controllerHost.IsLeader() = false, want true")
	}
}

func requireNoErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
