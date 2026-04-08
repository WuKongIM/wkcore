package raftcluster

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/groupcontroller"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
	"github.com/WuKongIM/WuKongIM/pkg/transport/nodetransport"
)

func TestObservationPeersForGroupPreferRuntimeMembership(t *testing.T) {
	cluster := &Cluster{
		cfg: Config{
			Groups: []GroupConfig{
				{GroupID: 7, Peers: []multiraft.NodeID{1, 2, 3}},
			},
		},
		assignments:  newAssignmentCache(),
		runtimePeers: make(map[multiraft.GroupID][]multiraft.NodeID),
	}
	cluster.assignments.SetAssignments([]controllermeta.GroupAssignment{
		{GroupID: 7, DesiredPeers: []uint64{2, 3}},
	})
	cluster.setRuntimePeers(7, []multiraft.NodeID{4, 5, 6})

	peers := cluster.observationPeersForGroup(7)
	if len(peers) != 3 || peers[0] != 4 || peers[1] != 5 || peers[2] != 6 {
		t.Fatalf("observationPeersForGroup() = %v", peers)
	}
}

func TestControllerReportAddrUsesBoundListener(t *testing.T) {
	srv := newStartedTestServer(t)

	cluster := &Cluster{
		cfg:    Config{ListenAddr: "127.0.0.1:0"},
		server: srv,
	}

	if got, want := cluster.controllerReportAddr(), srv.Listener().Addr().String(); got != want {
		t.Fatalf("controllerReportAddr() = %q, want %q", got, want)
	}
}

func TestListObservedRuntimeViewsUsesControllerClientWhenAvailable(t *testing.T) {
	dir := t.TempDir()
	store, err := controllermeta.Open(filepath.Join(dir, "controller-meta"))
	if err != nil {
		t.Fatalf("open controllermeta: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	if err := store.UpsertRuntimeView(context.Background(), controllermeta.GroupRuntimeView{
		GroupID:      1,
		CurrentPeers: []uint64{1, 2, 3},
		LastReportAt: time.Now(),
	}); err != nil {
		t.Fatalf("UpsertRuntimeView() error = %v", err)
	}

	sentinel := errors.New("controller client called")
	cluster := &Cluster{
		controllerMeta:   store,
		controllerClient: fakeControllerClient{listRuntimeViewsErr: sentinel},
	}

	_, err = cluster.ListObservedRuntimeViews(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("ListObservedRuntimeViews() error = %v, want %v", err, sentinel)
	}
}

type fakeControllerClient struct {
	reportErr           error
	assignments         []controllermeta.GroupAssignment
	assignmentsErr      error
	runtimeViews        []controllermeta.GroupRuntimeView
	listRuntimeViewsErr error
}

func (f fakeControllerClient) Report(_ context.Context, _ groupcontroller.AgentReport) error {
	return f.reportErr
}

func (f fakeControllerClient) RefreshAssignments(_ context.Context) ([]controllermeta.GroupAssignment, error) {
	return append([]controllermeta.GroupAssignment(nil), f.assignments...), f.assignmentsErr
}

func (f fakeControllerClient) ListRuntimeViews(_ context.Context) ([]controllermeta.GroupRuntimeView, error) {
	return append([]controllermeta.GroupRuntimeView(nil), f.runtimeViews...), f.listRuntimeViewsErr
}

func newStartedTestServer(t *testing.T) *nodetransport.Server {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_ = ln.Close()

	srv := nodetransport.NewServer()
	if err := srv.Start(ln.Addr().String()); err != nil {
		t.Fatalf("server.Start() error = %v", err)
	}
	t.Cleanup(srv.Stop)
	return srv
}
