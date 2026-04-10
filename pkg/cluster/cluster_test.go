package cluster

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
	raftstorage "github.com/WuKongIM/WuKongIM/pkg/raftlog"
	metadb "github.com/WuKongIM/WuKongIM/pkg/slot/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
)

func TestObservationPeersForGroupPreferRuntimeMembership(t *testing.T) {
	cluster := &Cluster{
		cfg: Config{
			Slots: []SlotConfig{
				{SlotID: 7, Peers: []multiraft.NodeID{1, 2, 3}},
			},
		},
		assignments:  newAssignmentCache(),
		runtimePeers: make(map[multiraft.SlotID][]multiraft.NodeID),
	}
	cluster.assignments.SetAssignments([]controllermeta.SlotAssignment{
		{SlotID: 7, DesiredPeers: []uint64{2, 3}},
	})
	cluster.setRuntimePeers(7, []multiraft.NodeID{4, 5, 6})

	peers := cluster.observationPeersForSlot(7)
	if len(peers) != 3 || peers[0] != 4 || peers[1] != 5 || peers[2] != 6 {
		t.Fatalf("observationPeersForSlot() = %v", peers)
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

	if err := store.UpsertRuntimeView(context.Background(), controllermeta.SlotRuntimeView{
		SlotID:       1,
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

func TestListObservedRuntimeViewsFallsBackToLocalControllerMetaWhenLeaderUnavailable(t *testing.T) {
	dir := t.TempDir()
	store, err := controllermeta.Open(filepath.Join(dir, "controller-meta"))
	if err != nil {
		t.Fatalf("open controllermeta: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	view := controllermeta.SlotRuntimeView{
		SlotID:       1,
		CurrentPeers: []uint64{1, 2, 3},
		LastReportAt: time.Now(),
	}
	if err := store.UpsertRuntimeView(context.Background(), view); err != nil {
		t.Fatalf("UpsertRuntimeView() error = %v", err)
	}

	cluster := &Cluster{
		controllerMeta:              store,
		controllerLeaderWaitTimeout: testControllerLeaderWaitTimeout,
		controllerClient:            fakeControllerClient{listRuntimeViewsErr: ErrNoLeader},
	}

	views, err := cluster.ListObservedRuntimeViews(context.Background())
	if err != nil {
		t.Fatalf("ListObservedRuntimeViews() error = %v", err)
	}
	if len(views) != 1 || views[0].SlotID != view.SlotID {
		t.Fatalf("ListObservedRuntimeViews() = %v", views)
	}
}

func TestListObservedRuntimeViewsFallsBackToLocalControllerMetaWhenControllerReadTimesOut(t *testing.T) {
	dir := t.TempDir()
	store, err := controllermeta.Open(filepath.Join(dir, "controller-meta"))
	if err != nil {
		t.Fatalf("open controllermeta: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	view := controllermeta.SlotRuntimeView{
		SlotID:       1,
		CurrentPeers: []uint64{1, 2, 3},
		LastReportAt: time.Now(),
	}
	if err := store.UpsertRuntimeView(context.Background(), view); err != nil {
		t.Fatalf("UpsertRuntimeView() error = %v", err)
	}
	cluster := &Cluster{
		controllerMeta:              store,
		controllerLeaderWaitTimeout: testControllerLeaderWaitTimeout,
		controllerClient:            fakeControllerClient{listRuntimeViewsErr: context.DeadlineExceeded},
	}

	views, err := cluster.ListObservedRuntimeViews(context.Background())
	if err != nil {
		t.Fatalf("ListObservedRuntimeViews() error = %v", err)
	}
	if len(views) != 1 || views[0].SlotID != view.SlotID {
		t.Fatalf("ListObservedRuntimeViews() = %v", views)
	}
}

func TestListSlotAssignmentsFallsBackToLocalControllerMetaWhenLeaderUnavailable(t *testing.T) {
	dir := t.TempDir()
	store, err := controllermeta.Open(filepath.Join(dir, "controller-meta"))
	if err != nil {
		t.Fatalf("open controllermeta: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	assignment := controllermeta.SlotAssignment{
		SlotID:       1,
		DesiredPeers: []uint64{1, 2, 3},
		ConfigEpoch:  1,
	}
	if err := store.UpsertAssignment(context.Background(), assignment); err != nil {
		t.Fatalf("UpsertAssignment() error = %v", err)
	}

	cluster := &Cluster{
		controllerMeta:              store,
		controllerLeaderWaitTimeout: testControllerLeaderWaitTimeout,
		controllerClient:            fakeControllerClient{assignmentsErr: ErrNoLeader},
	}

	assignments, err := cluster.ListSlotAssignments(context.Background())
	if err != nil {
		t.Fatalf("ListSlotAssignments() error = %v", err)
	}
	if len(assignments) != 1 || assignments[0].SlotID != assignment.SlotID {
		t.Fatalf("ListSlotAssignments() = %v", assignments)
	}
}

func TestListSlotAssignmentsFallsBackToLocalControllerMetaWhenControllerReadTimesOut(t *testing.T) {
	dir := t.TempDir()
	store, err := controllermeta.Open(filepath.Join(dir, "controller-meta"))
	if err != nil {
		t.Fatalf("open controllermeta: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	assignment := controllermeta.SlotAssignment{
		SlotID:       1,
		DesiredPeers: []uint64{1, 3, 4},
		ConfigEpoch:  2,
	}
	if err := store.UpsertAssignment(context.Background(), assignment); err != nil {
		t.Fatalf("UpsertAssignment() error = %v", err)
	}

	cluster := &Cluster{
		controllerMeta:              store,
		controllerLeaderWaitTimeout: testControllerLeaderWaitTimeout,
		controllerClient:            fakeControllerClient{assignmentsErr: context.DeadlineExceeded},
	}

	assignments, err := cluster.ListSlotAssignments(context.Background())
	if err != nil {
		t.Fatalf("ListSlotAssignments() error = %v", err)
	}
	if len(assignments) != 1 || assignments[0].SlotID != assignment.SlotID {
		t.Fatalf("ListSlotAssignments() = %v", assignments)
	}
}

func TestManagedSlotsReadyAllowsIdleLocalNode(t *testing.T) {
	cluster := &Cluster{
		cfg: Config{
			NodeID:    1,
			SlotCount: 2,
		},
		controllerClient: fakeControllerClient{
			assignments: []controllermeta.SlotAssignment{
				{SlotID: 1, DesiredPeers: []uint64{2, 3}},
				{SlotID: 2, DesiredPeers: []uint64{2, 3}},
			},
		},
	}

	ready, err := cluster.managedSlotsReady(context.Background())
	if err != nil {
		t.Fatalf("managedSlotsReady() error = %v", err)
	}
	if !ready {
		t.Fatal("managedSlotsReady() = false, want true when local node has no assigned slots")
	}
}

func TestLocalAssignedGroupIDsFiltersAssignmentsToLocalNode(t *testing.T) {
	cluster := &Cluster{
		cfg: Config{NodeID: 2},
	}

	slotIDs := cluster.localAssignedSlotIDs([]controllermeta.SlotAssignment{
		{SlotID: 1, DesiredPeers: []uint64{1, 2}},
		{SlotID: 2, DesiredPeers: []uint64{1, 3}},
		{SlotID: 3, DesiredPeers: []uint64{2, 3}},
	})

	want := []multiraft.SlotID{1, 3}
	if len(slotIDs) != len(want) {
		t.Fatalf("localAssignedSlotIDs() = %v, want %v", slotIDs, want)
	}
	for i := range want {
		if slotIDs[i] != want[i] {
			t.Fatalf("localAssignedSlotIDs() = %v, want %v", slotIDs, want)
		}
	}
}

func TestGetReconcileTaskFallsBackToLocalControllerMetaWhenLeaderUnavailable(t *testing.T) {
	dir := t.TempDir()
	store, err := controllermeta.Open(filepath.Join(dir, "controller-meta"))
	if err != nil {
		t.Fatalf("open controllermeta: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	task := controllermeta.ReconcileTask{
		SlotID:    1,
		Kind:      controllermeta.TaskKindBootstrap,
		Step:      controllermeta.TaskStepAddLearner,
		Status:    controllermeta.TaskStatusPending,
		NextRunAt: time.Now(),
	}
	if err := store.UpsertTask(context.Background(), task); err != nil {
		t.Fatalf("UpsertTask() error = %v", err)
	}

	cluster := &Cluster{
		controllerMeta:              store,
		controllerLeaderWaitTimeout: testControllerLeaderWaitTimeout,
		controllerClient:            fakeControllerClient{getTaskErr: ErrNoLeader},
	}

	got, err := cluster.GetReconcileTask(context.Background(), task.SlotID)
	if err != nil {
		t.Fatalf("GetReconcileTask() error = %v", err)
	}
	if got.SlotID != task.SlotID || got.Kind != task.Kind {
		t.Fatalf("GetReconcileTask() = %+v", got)
	}
}

func TestGetReconcileTaskFallsBackToLocalControllerMetaWhenControllerReadTimesOut(t *testing.T) {
	dir := t.TempDir()
	store, err := controllermeta.Open(filepath.Join(dir, "controller-meta"))
	if err != nil {
		t.Fatalf("open controllermeta: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	task := controllermeta.ReconcileTask{
		SlotID:     1,
		Kind:       controllermeta.TaskKindRepair,
		Step:       controllermeta.TaskStepAddLearner,
		SourceNode: 2,
		TargetNode: 4,
		Status:     controllermeta.TaskStatusRetrying,
		Attempt:    1,
		NextRunAt:  time.Now(),
	}
	if err := store.UpsertTask(context.Background(), task); err != nil {
		t.Fatalf("UpsertTask() error = %v", err)
	}

	cluster := &Cluster{
		controllerMeta:              store,
		controllerLeaderWaitTimeout: testControllerLeaderWaitTimeout,
		controllerClient:            fakeControllerClient{getTaskErr: context.DeadlineExceeded},
	}

	got, err := cluster.GetReconcileTask(context.Background(), task.SlotID)
	if err != nil {
		t.Fatalf("GetReconcileTask() error = %v", err)
	}
	if got.SlotID != task.SlotID || got.Kind != task.Kind || got.Attempt != task.Attempt {
		t.Fatalf("GetReconcileTask() = %+v", got)
	}
}

func TestGroupAgentSyncAssignmentsFallsBackToLocalControllerMetaWhenControllerReadTimesOut(t *testing.T) {
	dir := t.TempDir()
	store, err := controllermeta.Open(filepath.Join(dir, "controller-meta"))
	if err != nil {
		t.Fatalf("open controllermeta: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	assignment := controllermeta.SlotAssignment{
		SlotID:       1,
		DesiredPeers: []uint64{1, 3, 4},
		ConfigEpoch:  2,
	}
	if err := store.UpsertAssignment(context.Background(), assignment); err != nil {
		t.Fatalf("UpsertAssignment() error = %v", err)
	}

	cache := newAssignmentCache()
	cluster := &Cluster{
		controllerMeta:              store,
		controllerLeaderWaitTimeout: testControllerLeaderWaitTimeout,
		assignments:                 cache,
	}
	agent := &slotAgent{
		cluster: cluster,
		client:  fakeControllerClient{assignmentsErr: context.DeadlineExceeded},
		cache:   cache,
	}

	if err := agent.SyncAssignments(context.Background()); err != nil {
		t.Fatalf("SyncAssignments() error = %v", err)
	}
	assignments := cache.Snapshot()
	if len(assignments) != 1 || assignments[0].SlotID != assignment.SlotID {
		t.Fatalf("cached assignments = %v", assignments)
	}
}

type fakeControllerClient struct {
	reportErr            error
	listNodesFn          func(context.Context) ([]controllermeta.ClusterNode, error)
	nodes                []controllermeta.ClusterNode
	listNodesErr         error
	refreshAssignmentsFn func(context.Context) ([]controllermeta.SlotAssignment, error)
	assignments          []controllermeta.SlotAssignment
	assignmentsErr       error
	listRuntimeViewsFn   func(context.Context) ([]controllermeta.SlotRuntimeView, error)
	runtimeViews         []controllermeta.SlotRuntimeView
	listRuntimeViewsErr  error
	getTaskFn            func(context.Context, uint32) (controllermeta.ReconcileTask, error)
	tasks                map[uint32]controllermeta.ReconcileTask
	getTaskErr           error
	reportTaskResultErr  error
	reportTaskResultFn   func(context.Context, controllermeta.ReconcileTask, error) error
}

func (f fakeControllerClient) Report(_ context.Context, _ slotcontroller.AgentReport) error {
	return f.reportErr
}

func (f fakeControllerClient) ListNodes(ctx context.Context) ([]controllermeta.ClusterNode, error) {
	if f.listNodesFn != nil {
		return f.listNodesFn(ctx)
	}
	return append([]controllermeta.ClusterNode(nil), f.nodes...), f.listNodesErr
}

func (f fakeControllerClient) RefreshAssignments(ctx context.Context) ([]controllermeta.SlotAssignment, error) {
	if f.refreshAssignmentsFn != nil {
		return f.refreshAssignmentsFn(ctx)
	}
	return append([]controllermeta.SlotAssignment(nil), f.assignments...), f.assignmentsErr
}

func (f fakeControllerClient) ListRuntimeViews(ctx context.Context) ([]controllermeta.SlotRuntimeView, error) {
	if f.listRuntimeViewsFn != nil {
		return f.listRuntimeViewsFn(ctx)
	}
	return append([]controllermeta.SlotRuntimeView(nil), f.runtimeViews...), f.listRuntimeViewsErr
}

func (f fakeControllerClient) Operator(_ context.Context, _ slotcontroller.OperatorRequest) error {
	return nil
}

func (f fakeControllerClient) GetTask(ctx context.Context, slotID uint32) (controllermeta.ReconcileTask, error) {
	if f.getTaskFn != nil {
		return f.getTaskFn(ctx, slotID)
	}
	if f.getTaskErr != nil {
		return controllermeta.ReconcileTask{}, f.getTaskErr
	}
	if task, ok := f.tasks[slotID]; ok {
		return task, nil
	}
	return controllermeta.ReconcileTask{}, controllermeta.ErrNotFound
}

func (f fakeControllerClient) ForceReconcile(_ context.Context, _ uint32) error {
	return nil
}

func (f fakeControllerClient) ReportTaskResult(ctx context.Context, task controllermeta.ReconcileTask, taskErr error) error {
	if f.reportTaskResultFn != nil {
		return f.reportTaskResultFn(ctx, task, taskErr)
	}
	return f.reportTaskResultErr
}

func TestGroupAgentShouldExecuteTaskUsesLowestAliveAssignedPeerForBootstrap(t *testing.T) {
	agent := &slotAgent{
		cluster: &Cluster{cfg: Config{NodeID: 3}},
	}
	assignment := controllermeta.SlotAssignment{
		SlotID:       1,
		DesiredPeers: []uint64{2, 3, 4},
	}
	task := controllermeta.ReconcileTask{
		SlotID: 1,
		Kind:   controllermeta.TaskKindBootstrap,
		Status: controllermeta.TaskStatusPending,
	}
	nodes := map[uint64]controllermeta.ClusterNode{
		2: {NodeID: 2, Status: controllermeta.NodeStatusDead},
		3: {NodeID: 3, Status: controllermeta.NodeStatusAlive},
		4: {NodeID: 4, Status: controllermeta.NodeStatusAlive},
	}

	if !agent.shouldExecuteTask(assignment, task, nodes) {
		t.Fatal("shouldExecuteTask() = false, want true when local node is lowest alive peer")
	}
}

func TestGroupAgentShouldExecuteRepairTaskOnCurrentGroupLeader(t *testing.T) {
	restoreLeader := setManagedSlotLeaderTestHook(func(_ *Cluster, slotID multiraft.SlotID) (multiraft.NodeID, error, bool) {
		if slotID != 1 {
			return 0, nil, false
		}
		return 4, nil, true
	})
	defer restoreLeader()

	agent := &slotAgent{
		cluster: &Cluster{cfg: Config{NodeID: 4}},
	}
	assignment := controllermeta.SlotAssignment{
		SlotID:       1,
		DesiredPeers: []uint64{1, 2, 3},
	}
	task := controllermeta.ReconcileTask{
		SlotID:     1,
		Kind:       controllermeta.TaskKindRepair,
		SourceNode: 4,
		TargetNode: 3,
		Status:     controllermeta.TaskStatusPending,
	}

	if !agent.shouldExecuteTask(assignment, task, nil) {
		t.Fatal("shouldExecuteTask() = false, want true when local node is current slot leader")
	}
}

func TestGroupAgentShouldExecuteRepairTaskOnSourceNodeWhenSourceIsAlive(t *testing.T) {
	restoreLeader := setManagedSlotLeaderTestHook(func(_ *Cluster, slotID multiraft.SlotID) (multiraft.NodeID, error, bool) {
		if slotID != 1 {
			return 0, nil, false
		}
		return 3, nil, true
	})
	defer restoreLeader()

	agent := &slotAgent{
		cluster: &Cluster{cfg: Config{NodeID: 2}},
	}
	assignment := controllermeta.SlotAssignment{
		SlotID:       1,
		DesiredPeers: []uint64{1, 3, 4},
	}
	task := controllermeta.ReconcileTask{
		SlotID:     1,
		Kind:       controllermeta.TaskKindRepair,
		SourceNode: 2,
		TargetNode: 4,
		Status:     controllermeta.TaskStatusPending,
	}
	nodes := map[uint64]controllermeta.ClusterNode{
		2: {NodeID: 2, Status: controllermeta.NodeStatusDraining},
	}

	if !agent.shouldExecuteTask(assignment, task, nodes) {
		t.Fatal("shouldExecuteTask() = false, want true when local node is the alive source node")
	}
}

func TestGroupAgentShouldExecuteRepairTaskOnLocalSourceWithoutNodeSnapshot(t *testing.T) {
	restoreLeader := setManagedSlotLeaderTestHook(func(_ *Cluster, slotID multiraft.SlotID) (multiraft.NodeID, error, bool) {
		if slotID != 1 {
			return 0, nil, false
		}
		return 3, nil, true
	})
	defer restoreLeader()

	agent := &slotAgent{
		cluster: &Cluster{cfg: Config{NodeID: 2}},
	}
	assignment := controllermeta.SlotAssignment{
		SlotID:       1,
		DesiredPeers: []uint64{1, 3, 4},
	}
	task := controllermeta.ReconcileTask{
		SlotID:     1,
		Kind:       controllermeta.TaskKindRepair,
		SourceNode: 2,
		TargetNode: 4,
		Status:     controllermeta.TaskStatusRetrying,
		NextRunAt:  time.Now(),
	}

	if !agent.shouldExecuteTask(assignment, task, nil) {
		t.Fatal("shouldExecuteTask() = false, want true when local node is the task source")
	}
}

func TestGroupAgentShouldExecuteRepairTaskOnCurrentGroupLeaderWhenSourceUnavailable(t *testing.T) {
	restoreLeader := setManagedSlotLeaderTestHook(func(_ *Cluster, slotID multiraft.SlotID) (multiraft.NodeID, error, bool) {
		if slotID != 1 {
			return 0, nil, false
		}
		return 3, nil, true
	})
	defer restoreLeader()

	agent := &slotAgent{
		cluster: &Cluster{cfg: Config{NodeID: 3}},
	}
	assignment := controllermeta.SlotAssignment{
		SlotID:       1,
		DesiredPeers: []uint64{1, 3, 4},
	}
	task := controllermeta.ReconcileTask{
		SlotID:     1,
		Kind:       controllermeta.TaskKindRepair,
		SourceNode: 2,
		TargetNode: 4,
		Status:     controllermeta.TaskStatusPending,
	}
	nodes := map[uint64]controllermeta.ClusterNode{
		2: {NodeID: 2, Status: controllermeta.NodeStatusDead},
	}

	if !agent.shouldExecuteTask(assignment, task, nodes) {
		t.Fatal("shouldExecuteTask() = false, want true when source node is unavailable and local node is current slot leader")
	}
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

func TestWaitForManagedSlotCatchUpRequiresTargetToReachLeaderCommit(t *testing.T) {
	restoreLeader := setManagedSlotLeaderTestHook(func(_ *Cluster, slotID multiraft.SlotID) (multiraft.NodeID, error, bool) {
		if slotID != 1 {
			return 0, nil, false
		}
		return 1, nil, true
	})
	defer restoreLeader()

	restore := setManagedSlotStatusTestHook(func(_ *Cluster, nodeID multiraft.NodeID, slotID multiraft.SlotID) (managedSlotStatus, error, bool) {
		if slotID != 1 {
			return managedSlotStatus{}, nil, false
		}
		switch nodeID {
		case 1:
			return managedSlotStatus{LeaderID: 1, CommitIndex: 9, AppliedIndex: 9}, nil, true
		case 4:
			return managedSlotStatus{LeaderID: 1, CommitIndex: 9, AppliedIndex: 3}, nil, true
		default:
			return managedSlotStatus{}, ErrSlotNotFound, true
		}
	})
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := (&Cluster{}).waitForManagedSlotCatchUp(ctx, 1, 4)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForManagedSlotCatchUp() error = %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestWaitForManagedSlotCatchUpAllowsSlowLearnerCatchUp(t *testing.T) {
	restoreLeader := setManagedSlotLeaderTestHook(func(_ *Cluster, slotID multiraft.SlotID) (multiraft.NodeID, error, bool) {
		if slotID != 1 {
			return 0, nil, false
		}
		return 1, nil, true
	})
	defer restoreLeader()

	start := time.Now()
	restore := setManagedSlotStatusTestHook(func(_ *Cluster, nodeID multiraft.NodeID, slotID multiraft.SlotID) (managedSlotStatus, error, bool) {
		if slotID != 1 {
			return managedSlotStatus{}, nil, false
		}
		switch nodeID {
		case 1:
			return managedSlotStatus{LeaderID: 1, CommitIndex: 9, AppliedIndex: 9}, nil, true
		case 4:
			applied := uint64(3)
			if time.Since(start) >= 2300*time.Millisecond {
				applied = 9
			}
			return managedSlotStatus{LeaderID: 1, CommitIndex: 9, AppliedIndex: applied}, nil, true
		default:
			return managedSlotStatus{}, ErrSlotNotFound, true
		}
	})
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 3500*time.Millisecond)
	defer cancel()

	if err := (&Cluster{}).waitForManagedSlotCatchUp(ctx, 1, 4); err != nil {
		t.Fatalf("waitForManagedSlotCatchUp() error = %v, want nil", err)
	}
}

func newStartedTestServer(t *testing.T) *transport.Server {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_ = ln.Close()

	srv := transport.NewServer()
	if err := srv.Start(ln.Addr().String()); err != nil {
		t.Fatalf("server.Start() error = %v", err)
	}
	t.Cleanup(srv.Stop)
	return srv
}
