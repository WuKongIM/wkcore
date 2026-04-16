package cluster

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

func TestControllerHandlerAcceptsBinaryRequestBeforeHostChecks(t *testing.T) {
	handler := &controllerHandler{cluster: &Cluster{}}
	body, err := encodeControllerRequest(controllerRPCRequest{
		Kind: controllerRPCHeartbeat,
		Report: &slotcontroller.AgentReport{
			NodeID:         1,
			Addr:           "127.0.0.1:1111",
			ObservedAt:     time.Unix(1710000000, 0),
			CapacityWeight: 1,
		},
	})
	if err != nil {
		t.Fatalf("encodeControllerRequest() error = %v", err)
	}

	_, err = handler.Handle(context.Background(), body)
	if err != ErrNotStarted {
		t.Fatalf("controllerHandler.Handle() error = %v, want %v", err, ErrNotStarted)
	}
}

func TestControllerHandlerHeartbeatRedirectsFollower(t *testing.T) {
	cluster, _, _ := newTestLocalControllerCluster(t, false)
	handler := &controllerHandler{cluster: cluster}
	body, err := encodeControllerRequest(controllerRPCRequest{
		Kind: controllerRPCHeartbeat,
		Report: &slotcontroller.AgentReport{
			NodeID:     2,
			Addr:       "127.0.0.1:2222",
			ObservedAt: time.Unix(1710000100, 0),
		},
	})
	if err != nil {
		t.Fatalf("encodeControllerRequest() error = %v", err)
	}

	respBody, err := handler.Handle(context.Background(), body)
	if err != nil {
		t.Fatalf("controllerHandler.Handle() error = %v", err)
	}
	resp, err := decodeControllerResponse(controllerRPCHeartbeat, respBody)
	if err != nil {
		t.Fatalf("decodeControllerResponse() error = %v", err)
	}
	if !resp.NotLeader {
		t.Fatal("controllerHandler.Handle() NotLeader = false, want true")
	}
}

func TestControllerHandlerRuntimeReportRedirectsFollower(t *testing.T) {
	cluster, _, _ := newTestLocalControllerCluster(t, false)
	handler := &controllerHandler{cluster: cluster}
	body, err := encodeControllerRequest(controllerRPCRequest{
		Kind: controllerRPCRuntimeReport,
		RuntimeReport: &runtimeObservationReport{
			NodeID:     2,
			ObservedAt: time.Unix(1710000200, 0),
			FullSync:   true,
		},
	})
	if err != nil {
		t.Fatalf("encodeControllerRequest() error = %v", err)
	}

	respBody, err := handler.Handle(context.Background(), body)
	if err != nil {
		t.Fatalf("controllerHandler.Handle() error = %v", err)
	}
	resp, err := decodeControllerResponse(controllerRPCRuntimeReport, respBody)
	if err != nil {
		t.Fatalf("decodeControllerResponse() error = %v", err)
	}
	if !resp.NotLeader {
		t.Fatal("controllerHandler.Handle() NotLeader = false, want true")
	}
}

func TestControllerHandlerRuntimeReportUpdatesLeaderObservationWithoutProposal(t *testing.T) {
	cluster, host, _ := newTestLocalControllerCluster(t, true)
	handler := &controllerHandler{cluster: cluster}
	before, err := host.raftDB.ForController().LastIndex(context.Background())
	if err != nil {
		t.Fatalf("LastIndex(before) error = %v", err)
	}
	report := runtimeObservationReport{
		NodeID:     2,
		ObservedAt: time.Unix(1710000200, 0),
		FullSync:   true,
		Views: []controllermeta.SlotRuntimeView{{
			SlotID:              7,
			CurrentPeers:        []uint64{1, 2, 3},
			LeaderID:            2,
			HealthyVoters:       3,
			HasQuorum:           true,
			ObservedConfigEpoch: 9,
			LastReportAt:        time.Unix(1710000201, 0),
		}},
	}
	body, err := encodeControllerRequest(controllerRPCRequest{
		Kind:          controllerRPCRuntimeReport,
		RuntimeReport: &report,
	})
	if err != nil {
		t.Fatalf("encodeControllerRequest() error = %v", err)
	}

	respBody, err := handler.Handle(context.Background(), body)
	if err != nil {
		t.Fatalf("controllerHandler.Handle() error = %v", err)
	}
	resp, err := decodeControllerResponse(controllerRPCRuntimeReport, respBody)
	if err != nil {
		t.Fatalf("decodeControllerResponse() error = %v", err)
	}
	if resp.NotLeader {
		t.Fatal("controllerHandler.Handle() NotLeader = true, want false")
	}

	after, err := host.raftDB.ForController().LastIndex(context.Background())
	if err != nil {
		t.Fatalf("LastIndex(after) error = %v", err)
	}
	if after != before {
		t.Fatalf("LastIndex() after runtime report = %d, want unchanged %d", after, before)
	}

	snapshot := host.snapshotObservations()
	if len(snapshot.Nodes) != 0 {
		t.Fatalf("snapshot.Nodes = %#v", snapshot.Nodes)
	}
	if len(snapshot.RuntimeViews) != 1 || snapshot.RuntimeViews[0].SlotID != report.Views[0].SlotID {
		t.Fatalf("snapshot.RuntimeViews = %#v", snapshot.RuntimeViews)
	}
	views, err := host.meta.ListRuntimeViews(context.Background())
	if err != nil {
		t.Fatalf("ListRuntimeViews() error = %v", err)
	}
	if len(views) != 0 {
		t.Fatalf("ListRuntimeViews() = %#v, want runtime view to stay out of durable store", views)
	}
}

func TestControllerHandlerHeartbeatUpdatesNodeObservationOnly(t *testing.T) {
	cluster, host, _ := newTestLocalControllerCluster(t, true)
	handler := &controllerHandler{cluster: cluster}
	report := slotcontroller.AgentReport{
		NodeID:               2,
		Addr:                 "127.0.0.1:2222",
		ObservedAt:           time.Unix(1710000200, 0),
		CapacityWeight:       7,
		HashSlotTableVersion: 0,
		Runtime: &controllermeta.SlotRuntimeView{
			SlotID:              7,
			CurrentPeers:        []uint64{1, 2, 3},
			LeaderID:            2,
			HealthyVoters:       3,
			HasQuorum:           true,
			ObservedConfigEpoch: 9,
			LastReportAt:        time.Unix(1710000201, 0),
		},
	}
	body, err := encodeControllerRequest(controllerRPCRequest{
		Kind:   controllerRPCHeartbeat,
		Report: &report,
	})
	if err != nil {
		t.Fatalf("encodeControllerRequest() error = %v", err)
	}

	respBody, err := handler.Handle(context.Background(), body)
	if err != nil {
		t.Fatalf("controllerHandler.Handle() error = %v", err)
	}
	resp, err := decodeControllerResponse(controllerRPCHeartbeat, respBody)
	if err != nil {
		t.Fatalf("decodeControllerResponse() error = %v", err)
	}
	if resp.NotLeader {
		t.Fatal("controllerHandler.Handle() NotLeader = true, want false")
	}
	if resp.HashSlotTableVersion == 0 {
		t.Fatal("controllerHandler.Handle() HashSlotTableVersion = 0, want non-zero")
	}

	snapshot := host.snapshotObservations()
	if len(snapshot.Nodes) != 1 || snapshot.Nodes[0].NodeID != report.NodeID {
		t.Fatalf("snapshot.Nodes = %#v", snapshot.Nodes)
	}
	if len(snapshot.RuntimeViews) != 0 {
		t.Fatalf("snapshot.RuntimeViews = %#v, want no runtime views from heartbeat", snapshot.RuntimeViews)
	}

	nodes, err := host.meta.ListNodes(context.Background())
	if err != nil {
		t.Fatalf("ListNodes() error = %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeID != report.NodeID || nodes[0].Status != controllermeta.NodeStatusAlive {
		t.Fatalf("ListNodes() = %#v, want one alive durable status edge", nodes)
	}
}

func TestControllerHandlerHeartbeatUsesHashSlotSnapshotWhenWarm(t *testing.T) {
	cluster, host, _ := newTestLocalControllerCluster(t, true)
	handler := &controllerHandler{cluster: cluster}

	snapshot := NewHashSlotTable(8, 2)
	host.storeHashSlotTableSnapshot(snapshot)

	stored := snapshot.Clone()
	stored.StartMigration(3, 1, 2)
	requireNoErr(t, host.meta.SaveHashSlotTable(context.Background(), stored))

	body, err := encodeControllerRequest(controllerRPCRequest{
		Kind: controllerRPCHeartbeat,
		Report: &slotcontroller.AgentReport{
			NodeID:               2,
			Addr:                 "127.0.0.1:2222",
			ObservedAt:           time.Unix(1710000300, 0),
			HashSlotTableVersion: 0,
		},
	})
	if err != nil {
		t.Fatalf("encodeControllerRequest() error = %v", err)
	}

	respBody, err := handler.Handle(context.Background(), body)
	if err != nil {
		t.Fatalf("controllerHandler.Handle() error = %v", err)
	}
	resp, err := decodeControllerResponse(controllerRPCHeartbeat, respBody)
	if err != nil {
		t.Fatalf("decodeControllerResponse() error = %v", err)
	}
	if resp.HashSlotTableVersion != snapshot.Version() {
		t.Fatalf("HashSlotTableVersion = %d, want snapshot version %d", resp.HashSlotTableVersion, snapshot.Version())
	}
}

func TestControllerHandlerListAssignmentsUsesHashSlotSnapshotWhenWarm(t *testing.T) {
	cluster, host, _ := newTestLocalControllerCluster(t, true)
	handler := &controllerHandler{cluster: cluster}

	requireNoErr(t, host.meta.UpsertAssignment(context.Background(), controllermeta.SlotAssignment{
		SlotID:       1,
		DesiredPeers: []uint64{1},
		ConfigEpoch:  1,
	}))

	snapshot := NewHashSlotTable(8, 2)
	host.storeHashSlotTableSnapshot(snapshot)

	stored := snapshot.Clone()
	stored.StartMigration(3, 1, 2)
	requireNoErr(t, host.meta.SaveHashSlotTable(context.Background(), stored))

	body, err := encodeControllerRequest(controllerRPCRequest{Kind: controllerRPCListAssignments})
	if err != nil {
		t.Fatalf("encodeControllerRequest() error = %v", err)
	}

	respBody, err := handler.Handle(context.Background(), body)
	if err != nil {
		t.Fatalf("controllerHandler.Handle() error = %v", err)
	}
	resp, err := decodeControllerResponse(controllerRPCListAssignments, respBody)
	if err != nil {
		t.Fatalf("decodeControllerResponse() error = %v", err)
	}
	if len(resp.Assignments) != 1 || resp.Assignments[0].SlotID != 1 {
		t.Fatalf("Assignments = %#v, want slot 1 assignment", resp.Assignments)
	}
	if resp.HashSlotTableVersion != snapshot.Version() {
		t.Fatalf("HashSlotTableVersion = %d, want snapshot version %d", resp.HashSlotTableVersion, snapshot.Version())
	}
	table, err := DecodeHashSlotTable(resp.HashSlotTable)
	if err != nil {
		t.Fatalf("DecodeHashSlotTable() error = %v", err)
	}
	if table.Version() != snapshot.Version() {
		t.Fatalf("decoded table version = %d, want snapshot version %d", table.Version(), snapshot.Version())
	}
}

func TestControllerHandlerHeartbeatBackfillsHashSlotSnapshotOnColdMiss(t *testing.T) {
	cluster, host, _ := newTestLocalControllerCluster(t, true)
	handler := &controllerHandler{cluster: cluster}

	stored := NewHashSlotTable(8, 2)
	stored.StartMigration(3, 1, 2)
	requireNoErr(t, host.meta.SaveHashSlotTable(context.Background(), stored))

	body, err := encodeControllerRequest(controllerRPCRequest{
		Kind: controllerRPCHeartbeat,
		Report: &slotcontroller.AgentReport{
			NodeID:               2,
			Addr:                 "127.0.0.1:2222",
			ObservedAt:           time.Unix(1710000400, 0),
			HashSlotTableVersion: 0,
		},
	})
	if err != nil {
		t.Fatalf("encodeControllerRequest() error = %v", err)
	}

	respBody, err := handler.Handle(context.Background(), body)
	if err != nil {
		t.Fatalf("controllerHandler.Handle() error = %v", err)
	}
	resp, err := decodeControllerResponse(controllerRPCHeartbeat, respBody)
	if err != nil {
		t.Fatalf("decodeControllerResponse() error = %v", err)
	}
	if resp.HashSlotTableVersion != stored.Version() {
		t.Fatalf("HashSlotTableVersion = %d, want stored version %d", resp.HashSlotTableVersion, stored.Version())
	}

	snapshot, ok := host.hashSlotTableSnapshot()
	if !ok {
		t.Fatal("hashSlotTableSnapshot() ok = false, want true")
	}
	if snapshot.Version() != stored.Version() {
		t.Fatalf("hashSlotTableSnapshot().Version() = %d, want %d", snapshot.Version(), stored.Version())
	}
}

func newTestLocalControllerCluster(t *testing.T, start bool) (*Cluster, *controllerHost, *transportLayer) {
	t.Helper()

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
	t.Cleanup(host.Stop)

	if start {
		requireNoErr(t, host.Start(context.Background()))
		waitForTestControllerLeader(t, host, cfg.NodeID)
	}

	cluster := &Cluster{
		cfg:    cfg,
		router: NewRouter(NewHashSlotTable(cfg.effectiveHashSlotCount(), int(cfg.effectiveInitialSlotCount())), cfg.NodeID, nil),
		transportResources: transportResources{
			server: layer.server,
		},
		controllerResources: controllerResources{
			controllerHost: host,
			controllerMeta: host.meta,
			controller:     host.service,
		},
	}
	return cluster, host, layer
}

func waitForTestControllerLeader(t *testing.T, host *controllerHost, want multiraft.NodeID) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for host.LeaderID() != want && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if host.LeaderID() != want {
		t.Fatalf("controllerHost.LeaderID() = %d, want %d", host.LeaderID(), want)
	}
}
