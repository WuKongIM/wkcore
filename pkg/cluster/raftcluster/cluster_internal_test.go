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
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metafsm"
	"github.com/WuKongIM/WuKongIM/pkg/storage/raftstorage"
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
	tasks               map[uint32]controllermeta.ReconcileTask
	getTaskErr          error
	reportTaskResultErr error
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

func (f fakeControllerClient) Operator(_ context.Context, _ groupcontroller.OperatorRequest) error {
	return nil
}

func (f fakeControllerClient) GetTask(_ context.Context, groupID uint32) (controllermeta.ReconcileTask, error) {
	if f.getTaskErr != nil {
		return controllermeta.ReconcileTask{}, f.getTaskErr
	}
	if task, ok := f.tasks[groupID]; ok {
		return task, nil
	}
	return controllermeta.ReconcileTask{}, controllermeta.ErrNotFound
}

func (f fakeControllerClient) ForceReconcile(_ context.Context, _ uint32) error {
	return nil
}

func (f fakeControllerClient) ReportTaskResult(_ context.Context, _ uint32, _ error) error {
	return f.reportTaskResultErr
}

type standaloneAgentTestCluster struct {
	cluster *Cluster
	raftDB  *raftstorage.DB
	metaDB  *metadb.DB
}

func (c *standaloneAgentTestCluster) Close() {
	if c == nil {
		return
	}
	if c.cluster != nil {
		c.cluster.Stop()
		c.cluster = nil
	}
	if c.raftDB != nil {
		_ = c.raftDB.Close()
		c.raftDB = nil
	}
	if c.metaDB != nil {
		_ = c.metaDB.Close()
		c.metaDB = nil
	}
}

func newStandaloneAgentTestCluster(t *testing.T) *standaloneAgentTestCluster {
	t.Helper()

	dir := t.TempDir()
	metaDB, err := metadb.Open(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatalf("open metadb: %v", err)
	}
	raftDB, err := raftstorage.Open(filepath.Join(dir, "raft"))
	if err != nil {
		_ = metaDB.Close()
		t.Fatalf("open raftstorage: %v", err)
	}

	cluster, err := NewCluster(Config{
		NodeID:        1,
		ListenAddr:    "127.0.0.1:0",
		GroupCount:    1,
		GroupReplicaN: 1,
		Nodes: []NodeConfig{
			{NodeID: 1, Addr: "127.0.0.1:0"},
		},
		NewStorage: func(groupID multiraft.GroupID) (multiraft.Storage, error) {
			return raftDB.ForGroup(uint64(groupID)), nil
		},
		NewStateMachine: metafsm.NewStateMachineFactory(metaDB),
	})
	if err != nil {
		_ = raftDB.Close()
		_ = metaDB.Close()
		t.Fatalf("NewCluster() error = %v", err)
	}
	if err := cluster.Start(); err != nil {
		_ = raftDB.Close()
		_ = metaDB.Close()
		t.Fatalf("cluster.Start() error = %v", err)
	}

	t.Cleanup(func() {
		(&standaloneAgentTestCluster{
			cluster: cluster,
			raftDB:  raftDB,
			metaDB:  metaDB,
		}).Close()
	})

	return &standaloneAgentTestCluster{
		cluster: cluster,
		raftDB:  raftDB,
		metaDB:  metaDB,
	}
}

func TestGroupAgentSkipsBootstrapUntilBootstrapTaskExists(t *testing.T) {
	harness := newStandaloneAgentTestCluster(t)
	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1},
		ConfigEpoch:  1,
	}
	client := fakeControllerClient{
		assignments: []controllermeta.GroupAssignment{assignment},
	}
	harness.cluster.assignments.SetAssignments(client.assignments)
	harness.cluster.agent = &groupAgent{
		cluster: harness.cluster,
		client:  client,
		cache:   harness.cluster.assignments,
	}

	if err := harness.cluster.agent.ApplyAssignments(context.Background()); err != nil {
		t.Fatalf("ApplyAssignments() error = %v", err)
	}

	_, err := harness.cluster.runtime.Status(1)
	if !errors.Is(err, multiraft.ErrGroupNotFound) {
		t.Fatalf("runtime.Status() error = %v, want %v", err, multiraft.ErrGroupNotFound)
	}
}

func TestGroupAgentBootstrapsBrandNewGroupWhenBootstrapTaskExists(t *testing.T) {
	harness := newStandaloneAgentTestCluster(t)
	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1},
		ConfigEpoch:  1,
	}
	client := fakeControllerClient{
		assignments: []controllermeta.GroupAssignment{assignment},
		tasks: map[uint32]controllermeta.ReconcileTask{
			1: {
				GroupID:   1,
				Kind:      controllermeta.TaskKindBootstrap,
				Step:      controllermeta.TaskStepAddLearner,
				Status:    controllermeta.TaskStatusPending,
				NextRunAt: time.Now(),
			},
		},
	}
	harness.cluster.assignments.SetAssignments(client.assignments)
	harness.cluster.agent = &groupAgent{
		cluster: harness.cluster,
		client:  client,
		cache:   harness.cluster.assignments,
	}

	if err := harness.cluster.agent.ApplyAssignments(context.Background()); err != nil {
		t.Fatalf("ApplyAssignments() error = %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := harness.cluster.runtime.Status(1); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("group 1 was not bootstrapped")
}

func TestGroupAgentReopensPersistedGroupBeforeTaskFetch(t *testing.T) {
	harness := newStandaloneAgentTestCluster(t)
	requireNoError := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	requireNoError(harness.cluster.ensureManagedGroupLocal(context.Background(), 1, []uint64{1}, false, true))
	requireNoError(harness.cluster.waitForManagedGroupLeader(context.Background(), 1))
	requireNoError(harness.cluster.runtime.CloseGroup(context.Background(), 1))

	taskErr := errors.New("task fetch failed")
	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1},
		ConfigEpoch:  1,
	}
	client := fakeControllerClient{
		assignments: []controllermeta.GroupAssignment{assignment},
		getTaskErr:  taskErr,
	}
	harness.cluster.assignments.SetAssignments(client.assignments)
	harness.cluster.agent = &groupAgent{
		cluster: harness.cluster,
		client:  client,
		cache:   harness.cluster.assignments,
	}

	err := harness.cluster.agent.ApplyAssignments(context.Background())
	if !errors.Is(err, taskErr) {
		t.Fatalf("ApplyAssignments() error = %v, want %v", err, taskErr)
	}
	if _, err := harness.cluster.runtime.Status(1); err != nil {
		t.Fatalf("runtime.Status() error = %v", err)
	}
}

func TestWaitForManagedGroupCatchUpRequiresTargetToReachLeaderCommit(t *testing.T) {
	restoreLeader := setManagedGroupLeaderTestHook(func(_ *Cluster, groupID multiraft.GroupID) (multiraft.NodeID, error, bool) {
		if groupID != 1 {
			return 0, nil, false
		}
		return 1, nil, true
	})
	defer restoreLeader()

	restore := setManagedGroupStatusTestHook(func(_ *Cluster, nodeID multiraft.NodeID, groupID multiraft.GroupID) (managedGroupStatus, error, bool) {
		if groupID != 1 {
			return managedGroupStatus{}, nil, false
		}
		switch nodeID {
		case 1:
			return managedGroupStatus{LeaderID: 1, CommitIndex: 9, AppliedIndex: 9}, nil, true
		case 4:
			return managedGroupStatus{LeaderID: 1, CommitIndex: 9, AppliedIndex: 3}, nil, true
		default:
			return managedGroupStatus{}, ErrGroupNotFound, true
		}
	})
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := (&Cluster{}).waitForManagedGroupCatchUp(ctx, 1, 4)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForManagedGroupCatchUp() error = %v, want %v", err, context.DeadlineExceeded)
	}
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
