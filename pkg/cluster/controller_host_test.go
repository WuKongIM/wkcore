package cluster

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
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
	if host.observations == nil {
		t.Fatal("newControllerHost() observations = nil")
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

func TestControllerHostHashSlotSnapshotReloadsOnLocalLeaderChange(t *testing.T) {
	_, host, _ := newTestLocalControllerCluster(t, false)

	table := NewHashSlotTable(8, 2)
	requireNoErr(t, host.meta.SaveHashSlotTable(context.Background(), table))

	host.handleLeaderChange(2, host.localNode)

	snapshot, ok := host.hashSlotTableSnapshot()
	if !ok {
		t.Fatal("hashSlotTableSnapshot() ok = false, want true")
	}
	if snapshot.Version() != table.Version() {
		t.Fatalf("hashSlotTableSnapshot().Version() = %d, want %d", snapshot.Version(), table.Version())
	}
}

func TestControllerHostHashSlotSnapshotClearsOnLeaderLoss(t *testing.T) {
	_, host, _ := newTestLocalControllerCluster(t, false)

	host.storeHashSlotTableSnapshot(NewHashSlotTable(8, 2))
	if _, ok := host.hashSlotTableSnapshot(); !ok {
		t.Fatal("hashSlotTableSnapshot() ok = false before leader loss, want true")
	}

	host.handleLeaderChange(host.localNode, 2)

	if _, ok := host.hashSlotTableSnapshot(); ok {
		t.Fatal("hashSlotTableSnapshot() ok = true after leader loss, want false")
	}
}

func TestControllerHostHandleCommittedCommandReloadsHashSlotSnapshotOnHashSlotMutation(t *testing.T) {
	_, host, _ := newTestLocalControllerCluster(t, false)

	initial := NewHashSlotTable(8, 2)
	requireNoErr(t, host.meta.SaveHashSlotTable(context.Background(), initial))
	host.storeHashSlotTableSnapshot(initial)

	updated := initial.Clone()
	updated.StartMigration(3, 1, 2)
	requireNoErr(t, host.meta.SaveHashSlotTable(context.Background(), updated))

	host.handleCommittedCommand(slotcontroller.Command{
		Kind: slotcontroller.CommandKindStartMigration,
		Migration: &slotcontroller.MigrationRequest{
			HashSlot: 3,
			Source:   1,
			Target:   2,
		},
	})

	snapshot, ok := host.hashSlotTableSnapshot()
	if !ok {
		t.Fatal("hashSlotTableSnapshot() ok = false, want true")
	}
	if snapshot.Version() != updated.Version() {
		t.Fatalf("hashSlotTableSnapshot().Version() = %d, want %d", snapshot.Version(), updated.Version())
	}
	if migration := snapshot.GetMigration(3); migration == nil {
		t.Fatal("hashSlotTableSnapshot().GetMigration(3) = nil, want active migration")
	}
}

func TestControllerHostLeaderChangeReloadsNodeMirrorOnLocalLeadership(t *testing.T) {
	_, host, _ := newTestLocalControllerCluster(t, false)
	requireNoErr(t, host.meta.UpsertNode(context.Background(), controllermeta.ClusterNode{
		NodeID:          1,
		Addr:            "127.0.0.1:7001",
		Status:          controllermeta.NodeStatusAlive,
		LastHeartbeatAt: time.Unix(1710000000, 0),
		CapacityWeight:  1,
	}))

	host.handleLeaderChange(2, host.localNode)

	node, ok := host.healthScheduler.mirroredNode(1)
	if !ok {
		t.Fatal("mirroredNode(1) ok = false, want true")
	}
	if node.Status != controllermeta.NodeStatusAlive {
		t.Fatalf("mirroredNode(1).Status = %v, want alive", node.Status)
	}
}

func TestControllerHostLeaderChangeClearsNodeMirrorOnLeaderLoss(t *testing.T) {
	_, host, _ := newTestLocalControllerCluster(t, false)
	host.healthScheduler.mirrorNode(controllermeta.ClusterNode{
		NodeID:         1,
		Addr:           "127.0.0.1:7001",
		Status:         controllermeta.NodeStatusAlive,
		CapacityWeight: 1,
	})

	host.handleLeaderChange(host.localNode, 2)

	if _, ok := host.healthScheduler.mirroredNode(1); ok {
		t.Fatal("mirroredNode(1) ok = true after leader loss, want false")
	}
}

func TestControllerHostWarmupRequiresRuntimeFullSync(t *testing.T) {
	_, host, _ := newTestLocalControllerCluster(t, true)
	requireNoErr(t, host.meta.UpsertNode(context.Background(), controllermeta.ClusterNode{
		NodeID:          1,
		Addr:            "127.0.0.1:7001",
		Status:          controllermeta.NodeStatusAlive,
		LastHeartbeatAt: time.Unix(1710001000, 0),
		CapacityWeight:  1,
	}))
	requireNoErr(t, host.meta.UpsertNode(context.Background(), controllermeta.ClusterNode{
		NodeID:          2,
		Addr:            "127.0.0.1:7002",
		Status:          controllermeta.NodeStatusAlive,
		LastHeartbeatAt: time.Unix(1710001000, 0),
		CapacityWeight:  1,
	}))

	if host.warmupComplete() {
		t.Fatal("warmupComplete() = true before runtime full sync, want false")
	}

	host.applyRuntimeReport(runtimeObservationReport{
		NodeID:     1,
		ObservedAt: time.Unix(1710001001, 0),
		FullSync:   true,
	})
	if host.warmupComplete() {
		t.Fatal("warmupComplete() = true after only one node full sync, want false")
	}

	host.applyRuntimeReport(runtimeObservationReport{
		NodeID:     2,
		ObservedAt: time.Unix(1710001002, 0),
		FullSync:   true,
	})
	if !host.warmupComplete() {
		t.Fatal("warmupComplete() = false after all alive nodes full sync, want true")
	}
}

func TestControllerHostLeaderChangeResetsRuntimeWarmupCoverage(t *testing.T) {
	_, host, _ := newTestLocalControllerCluster(t, true)
	requireNoErr(t, host.meta.UpsertNode(context.Background(), controllermeta.ClusterNode{
		NodeID:          1,
		Addr:            "127.0.0.1:7001",
		Status:          controllermeta.NodeStatusAlive,
		LastHeartbeatAt: time.Unix(1710001100, 0),
		CapacityWeight:  1,
	}))

	host.applyRuntimeReport(runtimeObservationReport{
		NodeID:     1,
		ObservedAt: time.Unix(1710001101, 0),
		FullSync:   true,
	})
	if !host.warmupComplete() {
		t.Fatal("warmupComplete() = false after local full sync, want true")
	}

	host.handleLeaderChange(host.localNode, 2)
	host.handleLeaderChange(2, host.localNode)
	if host.warmupComplete() {
		t.Fatal("warmupComplete() = true after leader change reset, want false")
	}

	host.applyRuntimeReport(runtimeObservationReport{
		NodeID:     1,
		ObservedAt: time.Unix(1710001102, 0),
		FullSync:   true,
	})
	if !host.warmupComplete() {
		t.Fatal("warmupComplete() = false after post-reset full sync, want true")
	}
}
