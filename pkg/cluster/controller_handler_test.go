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

func TestControllerHandlerHeartbeatUpdatesLeaderObservationWithoutProposal(t *testing.T) {
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
	if len(snapshot.RuntimeViews) != 1 || snapshot.RuntimeViews[0].SlotID != report.Runtime.SlotID {
		t.Fatalf("snapshot.RuntimeViews = %#v", snapshot.RuntimeViews)
	}

	nodes, err := host.meta.ListNodes(context.Background())
	if err != nil {
		t.Fatalf("ListNodes() error = %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("ListNodes() = %#v, want empty store state", nodes)
	}
	views, err := host.meta.ListRuntimeViews(context.Background())
	if err != nil {
		t.Fatalf("ListRuntimeViews() error = %v", err)
	}
	if len(views) != 0 {
		t.Fatalf("ListRuntimeViews() = %#v, want empty store state", views)
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
