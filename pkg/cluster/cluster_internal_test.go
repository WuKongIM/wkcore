package cluster

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	groupcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
	raftstorage "github.com/WuKongIM/WuKongIM/pkg/raftlog"
	metafsm "github.com/WuKongIM/WuKongIM/pkg/slot/fsm"
	metadb "github.com/WuKongIM/WuKongIM/pkg/slot/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
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

func TestListObservedRuntimeViewsFallsBackToLocalControllerMetaWhenLeaderUnavailable(t *testing.T) {
	dir := t.TempDir()
	store, err := controllermeta.Open(filepath.Join(dir, "controller-meta"))
	if err != nil {
		t.Fatalf("open controllermeta: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	view := controllermeta.GroupRuntimeView{
		GroupID:      1,
		CurrentPeers: []uint64{1, 2, 3},
		LastReportAt: time.Now(),
	}
	if err := store.UpsertRuntimeView(context.Background(), view); err != nil {
		t.Fatalf("UpsertRuntimeView() error = %v", err)
	}

	cluster := &Cluster{
		controllerMeta:   store,
		controllerClient: fakeControllerClient{listRuntimeViewsErr: ErrNoLeader},
	}

	views, err := cluster.ListObservedRuntimeViews(context.Background())
	if err != nil {
		t.Fatalf("ListObservedRuntimeViews() error = %v", err)
	}
	if len(views) != 1 || views[0].GroupID != view.GroupID {
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

	view := controllermeta.GroupRuntimeView{
		GroupID:      1,
		CurrentPeers: []uint64{1, 2, 3},
		LastReportAt: time.Now(),
	}
	if err := store.UpsertRuntimeView(context.Background(), view); err != nil {
		t.Fatalf("UpsertRuntimeView() error = %v", err)
	}

	cluster := &Cluster{
		controllerMeta:   store,
		controllerClient: fakeControllerClient{listRuntimeViewsErr: context.DeadlineExceeded},
	}

	views, err := cluster.ListObservedRuntimeViews(context.Background())
	if err != nil {
		t.Fatalf("ListObservedRuntimeViews() error = %v", err)
	}
	if len(views) != 1 || views[0].GroupID != view.GroupID {
		t.Fatalf("ListObservedRuntimeViews() = %v", views)
	}
}

func TestListGroupAssignmentsFallsBackToLocalControllerMetaWhenLeaderUnavailable(t *testing.T) {
	dir := t.TempDir()
	store, err := controllermeta.Open(filepath.Join(dir, "controller-meta"))
	if err != nil {
		t.Fatalf("open controllermeta: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1, 2, 3},
		ConfigEpoch:  1,
	}
	if err := store.UpsertAssignment(context.Background(), assignment); err != nil {
		t.Fatalf("UpsertAssignment() error = %v", err)
	}

	cluster := &Cluster{
		controllerMeta:   store,
		controllerClient: fakeControllerClient{assignmentsErr: ErrNoLeader},
	}

	assignments, err := cluster.ListGroupAssignments(context.Background())
	if err != nil {
		t.Fatalf("ListGroupAssignments() error = %v", err)
	}
	if len(assignments) != 1 || assignments[0].GroupID != assignment.GroupID {
		t.Fatalf("ListGroupAssignments() = %v", assignments)
	}
}

func TestListGroupAssignmentsFallsBackToLocalControllerMetaWhenControllerReadTimesOut(t *testing.T) {
	dir := t.TempDir()
	store, err := controllermeta.Open(filepath.Join(dir, "controller-meta"))
	if err != nil {
		t.Fatalf("open controllermeta: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1, 3, 4},
		ConfigEpoch:  2,
	}
	if err := store.UpsertAssignment(context.Background(), assignment); err != nil {
		t.Fatalf("UpsertAssignment() error = %v", err)
	}

	cluster := &Cluster{
		controllerMeta:   store,
		controllerClient: fakeControllerClient{assignmentsErr: context.DeadlineExceeded},
	}

	assignments, err := cluster.ListGroupAssignments(context.Background())
	if err != nil {
		t.Fatalf("ListGroupAssignments() error = %v", err)
	}
	if len(assignments) != 1 || assignments[0].GroupID != assignment.GroupID {
		t.Fatalf("ListGroupAssignments() = %v", assignments)
	}
}

func TestManagedGroupsReadyAllowsIdleLocalNode(t *testing.T) {
	cluster := &Cluster{
		cfg: Config{
			NodeID:     1,
			GroupCount: 2,
		},
		controllerClient: fakeControllerClient{
			assignments: []controllermeta.GroupAssignment{
				{GroupID: 1, DesiredPeers: []uint64{2, 3}},
				{GroupID: 2, DesiredPeers: []uint64{2, 3}},
			},
		},
	}

	ready, err := cluster.managedGroupsReady(context.Background())
	if err != nil {
		t.Fatalf("managedGroupsReady() error = %v", err)
	}
	if !ready {
		t.Fatal("managedGroupsReady() = false, want true when local node has no assigned groups")
	}
}

func TestLocalAssignedGroupIDsFiltersAssignmentsToLocalNode(t *testing.T) {
	cluster := &Cluster{
		cfg: Config{NodeID: 2},
	}

	groupIDs := cluster.localAssignedGroupIDs([]controllermeta.GroupAssignment{
		{GroupID: 1, DesiredPeers: []uint64{1, 2}},
		{GroupID: 2, DesiredPeers: []uint64{1, 3}},
		{GroupID: 3, DesiredPeers: []uint64{2, 3}},
	})

	want := []multiraft.GroupID{1, 3}
	if len(groupIDs) != len(want) {
		t.Fatalf("localAssignedGroupIDs() = %v, want %v", groupIDs, want)
	}
	for i := range want {
		if groupIDs[i] != want[i] {
			t.Fatalf("localAssignedGroupIDs() = %v, want %v", groupIDs, want)
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
		GroupID:   1,
		Kind:      controllermeta.TaskKindBootstrap,
		Step:      controllermeta.TaskStepAddLearner,
		Status:    controllermeta.TaskStatusPending,
		NextRunAt: time.Now(),
	}
	if err := store.UpsertTask(context.Background(), task); err != nil {
		t.Fatalf("UpsertTask() error = %v", err)
	}

	cluster := &Cluster{
		controllerMeta:   store,
		controllerClient: fakeControllerClient{getTaskErr: ErrNoLeader},
	}

	got, err := cluster.GetReconcileTask(context.Background(), task.GroupID)
	if err != nil {
		t.Fatalf("GetReconcileTask() error = %v", err)
	}
	if got.GroupID != task.GroupID || got.Kind != task.Kind {
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
		GroupID:    1,
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
		controllerMeta:   store,
		controllerClient: fakeControllerClient{getTaskErr: context.DeadlineExceeded},
	}

	got, err := cluster.GetReconcileTask(context.Background(), task.GroupID)
	if err != nil {
		t.Fatalf("GetReconcileTask() error = %v", err)
	}
	if got.GroupID != task.GroupID || got.Kind != task.Kind || got.Attempt != task.Attempt {
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

	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1, 3, 4},
		ConfigEpoch:  2,
	}
	if err := store.UpsertAssignment(context.Background(), assignment); err != nil {
		t.Fatalf("UpsertAssignment() error = %v", err)
	}

	cache := newAssignmentCache()
	cluster := &Cluster{
		controllerMeta: store,
		assignments:    cache,
	}
	agent := &groupAgent{
		cluster: cluster,
		client:  fakeControllerClient{assignmentsErr: context.DeadlineExceeded},
		cache:   cache,
	}

	if err := agent.SyncAssignments(context.Background()); err != nil {
		t.Fatalf("SyncAssignments() error = %v", err)
	}
	assignments := cache.Snapshot()
	if len(assignments) != 1 || assignments[0].GroupID != assignment.GroupID {
		t.Fatalf("cached assignments = %v", assignments)
	}
}

func TestGroupAgentApplyAssignmentsFallsBackToLocalControllerTaskWhenControllerReadTimesOut(t *testing.T) {
	harness := newStandaloneAgentTestCluster(t)

	dir := t.TempDir()
	store, err := controllermeta.Open(filepath.Join(dir, "controller-meta"))
	if err != nil {
		t.Fatalf("open controllermeta: %v", err)
	}
	harness.cluster.controllerMeta = store

	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1},
		ConfigEpoch:  1,
	}
	task := controllermeta.ReconcileTask{
		GroupID:   1,
		Kind:      controllermeta.TaskKindBootstrap,
		Step:      controllermeta.TaskStepAddLearner,
		Status:    controllermeta.TaskStatusPending,
		NextRunAt: time.Now(),
	}
	if err := store.UpsertAssignment(context.Background(), assignment); err != nil {
		t.Fatalf("UpsertAssignment() error = %v", err)
	}
	if err := store.UpsertTask(context.Background(), task); err != nil {
		t.Fatalf("UpsertTask() error = %v", err)
	}

	harness.cluster.assignments.SetAssignments([]controllermeta.GroupAssignment{assignment})
	harness.cluster.agent = &groupAgent{
		cluster: harness.cluster,
		client: fakeControllerClient{
			listNodesErr:        context.DeadlineExceeded,
			listRuntimeViewsErr: context.DeadlineExceeded,
			getTaskErr:          context.DeadlineExceeded,
		},
		cache: harness.cluster.assignments,
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

func TestObserveOnceAppliesCachedAssignmentsWhenSyncAssignmentsTimesOut(t *testing.T) {
	harness := newStandaloneAgentTestCluster(t)

	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1},
		ConfigEpoch:  1,
	}
	task := controllermeta.ReconcileTask{
		GroupID:   1,
		Kind:      controllermeta.TaskKindBootstrap,
		Step:      controllermeta.TaskStepAddLearner,
		Status:    controllermeta.TaskStatusPending,
		NextRunAt: time.Now(),
	}

	reportCalls := 0
	agent := &groupAgent{
		cluster: harness.cluster,
		client: fakeControllerClient{
			assignmentsErr: context.DeadlineExceeded,
			tasks:          map[uint32]controllermeta.ReconcileTask{1: task},
			reportTaskResultFn: func(_ context.Context, task controllermeta.ReconcileTask, taskErr error) error {
				reportCalls++
				if task.GroupID != 1 {
					t.Fatalf("ReportTaskResult() groupID = %d, want 1", task.GroupID)
				}
				if taskErr != nil {
					t.Fatalf("ReportTaskResult() err = %v, want nil", taskErr)
				}
				return nil
			},
		},
		cache: harness.cluster.assignments,
	}
	harness.cluster.assignments.SetAssignments([]controllermeta.GroupAssignment{assignment})
	agent.storePendingTaskReport(1, task, nil)
	harness.cluster.agent = agent

	harness.cluster.observeOnce(context.Background())

	if reportCalls != 1 {
		t.Fatalf("ReportTaskResult() calls = %d, want 1", reportCalls)
	}
	if _, ok := agent.pendingTaskReport(1); ok {
		t.Fatal("pending task report was not cleared")
	}
}

type fakeControllerClient struct {
	reportErr            error
	listNodesFn          func(context.Context) ([]controllermeta.ClusterNode, error)
	nodes                []controllermeta.ClusterNode
	listNodesErr         error
	refreshAssignmentsFn func(context.Context) ([]controllermeta.GroupAssignment, error)
	assignments          []controllermeta.GroupAssignment
	assignmentsErr       error
	listRuntimeViewsFn   func(context.Context) ([]controllermeta.GroupRuntimeView, error)
	runtimeViews         []controllermeta.GroupRuntimeView
	listRuntimeViewsErr  error
	getTaskFn            func(context.Context, uint32) (controllermeta.ReconcileTask, error)
	tasks                map[uint32]controllermeta.ReconcileTask
	getTaskErr           error
	reportTaskResultErr  error
	reportTaskResultFn   func(context.Context, controllermeta.ReconcileTask, error) error
}

func (f fakeControllerClient) Report(_ context.Context, _ groupcontroller.AgentReport) error {
	return f.reportErr
}

func (f fakeControllerClient) ListNodes(ctx context.Context) ([]controllermeta.ClusterNode, error) {
	if f.listNodesFn != nil {
		return f.listNodesFn(ctx)
	}
	return append([]controllermeta.ClusterNode(nil), f.nodes...), f.listNodesErr
}

func (f fakeControllerClient) RefreshAssignments(ctx context.Context) ([]controllermeta.GroupAssignment, error) {
	if f.refreshAssignmentsFn != nil {
		return f.refreshAssignmentsFn(ctx)
	}
	return append([]controllermeta.GroupAssignment(nil), f.assignments...), f.assignmentsErr
}

func (f fakeControllerClient) ListRuntimeViews(ctx context.Context) ([]controllermeta.GroupRuntimeView, error) {
	if f.listRuntimeViewsFn != nil {
		return f.listRuntimeViewsFn(ctx)
	}
	return append([]controllermeta.GroupRuntimeView(nil), f.runtimeViews...), f.listRuntimeViewsErr
}

func (f fakeControllerClient) Operator(_ context.Context, _ groupcontroller.OperatorRequest) error {
	return nil
}

func (f fakeControllerClient) GetTask(ctx context.Context, groupID uint32) (controllermeta.ReconcileTask, error) {
	if f.getTaskFn != nil {
		return f.getTaskFn(ctx, groupID)
	}
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

func (f fakeControllerClient) ReportTaskResult(ctx context.Context, task controllermeta.ReconcileTask, taskErr error) error {
	if f.reportTaskResultFn != nil {
		return f.reportTaskResultFn(ctx, task, taskErr)
	}
	return f.reportTaskResultErr
}

func TestGroupAgentShouldExecuteTaskUsesLowestAliveAssignedPeerForBootstrap(t *testing.T) {
	agent := &groupAgent{
		cluster: &Cluster{cfg: Config{NodeID: 3}},
	}
	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{2, 3, 4},
	}
	task := controllermeta.ReconcileTask{
		GroupID: 1,
		Kind:    controllermeta.TaskKindBootstrap,
		Status:  controllermeta.TaskStatusPending,
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
	restoreLeader := setManagedGroupLeaderTestHook(func(_ *Cluster, groupID multiraft.GroupID) (multiraft.NodeID, error, bool) {
		if groupID != 1 {
			return 0, nil, false
		}
		return 4, nil, true
	})
	defer restoreLeader()

	agent := &groupAgent{
		cluster: &Cluster{cfg: Config{NodeID: 4}},
	}
	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1, 2, 3},
	}
	task := controllermeta.ReconcileTask{
		GroupID:    1,
		Kind:       controllermeta.TaskKindRepair,
		SourceNode: 4,
		TargetNode: 3,
		Status:     controllermeta.TaskStatusPending,
	}

	if !agent.shouldExecuteTask(assignment, task, nil) {
		t.Fatal("shouldExecuteTask() = false, want true when local node is current group leader")
	}
}

func TestGroupAgentShouldExecuteRepairTaskOnSourceNodeWhenSourceIsAlive(t *testing.T) {
	restoreLeader := setManagedGroupLeaderTestHook(func(_ *Cluster, groupID multiraft.GroupID) (multiraft.NodeID, error, bool) {
		if groupID != 1 {
			return 0, nil, false
		}
		return 3, nil, true
	})
	defer restoreLeader()

	agent := &groupAgent{
		cluster: &Cluster{cfg: Config{NodeID: 2}},
	}
	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1, 3, 4},
	}
	task := controllermeta.ReconcileTask{
		GroupID:    1,
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
	restoreLeader := setManagedGroupLeaderTestHook(func(_ *Cluster, groupID multiraft.GroupID) (multiraft.NodeID, error, bool) {
		if groupID != 1 {
			return 0, nil, false
		}
		return 3, nil, true
	})
	defer restoreLeader()

	agent := &groupAgent{
		cluster: &Cluster{cfg: Config{NodeID: 2}},
	}
	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1, 3, 4},
	}
	task := controllermeta.ReconcileTask{
		GroupID:    1,
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
	restoreLeader := setManagedGroupLeaderTestHook(func(_ *Cluster, groupID multiraft.GroupID) (multiraft.NodeID, error, bool) {
		if groupID != 1 {
			return 0, nil, false
		}
		return 3, nil, true
	})
	defer restoreLeader()

	agent := &groupAgent{
		cluster: &Cluster{cfg: Config{NodeID: 3}},
	}
	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1, 3, 4},
	}
	task := controllermeta.ReconcileTask{
		GroupID:    1,
		Kind:       controllermeta.TaskKindRepair,
		SourceNode: 2,
		TargetNode: 4,
		Status:     controllermeta.TaskStatusPending,
	}
	nodes := map[uint64]controllermeta.ClusterNode{
		2: {NodeID: 2, Status: controllermeta.NodeStatusDead},
	}

	if !agent.shouldExecuteTask(assignment, task, nodes) {
		t.Fatal("shouldExecuteTask() = false, want true when source node is unavailable and local node is current group leader")
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

func TestGroupAgentKeepsSourceGroupOpenWhileRepairTaskPending(t *testing.T) {
	harness := newStandaloneAgentTestCluster(t)
	requireNoError := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	requireNoError(harness.cluster.ensureManagedGroupLocal(context.Background(), 1, []uint64{1}, false, true))
	requireNoError(harness.cluster.waitForManagedGroupLeader(context.Background(), 1))

	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{2, 3},
		ConfigEpoch:  2,
	}
	client := fakeControllerClient{
		assignments: []controllermeta.GroupAssignment{assignment},
		tasks: map[uint32]controllermeta.ReconcileTask{
			1: {
				GroupID:    1,
				Kind:       controllermeta.TaskKindRepair,
				SourceNode: 1,
				TargetNode: 2,
				Status:     controllermeta.TaskStatusPending,
				NextRunAt:  time.Now(),
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
	if _, err := harness.cluster.runtime.Status(1); err != nil {
		t.Fatalf("runtime.Status() error = %v, want source group to remain open while repair is pending", err)
	}
}

func TestGroupAgentRetriesTaskResultReportOnControllerLeaderChange(t *testing.T) {
	harness := newStandaloneAgentTestCluster(t)
	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1},
		ConfigEpoch:  1,
	}
	task := controllermeta.ReconcileTask{
		GroupID:   1,
		Kind:      controllermeta.TaskKindBootstrap,
		Step:      controllermeta.TaskStepAddLearner,
		Status:    controllermeta.TaskStatusPending,
		NextRunAt: time.Now(),
	}
	execErr := errors.New("injected execution failure")
	restore := SetManagedGroupExecutionTestHook(func(groupID uint32, got controllermeta.ReconcileTask) error {
		if groupID == 1 && got.Kind == controllermeta.TaskKindBootstrap {
			return execErr
		}
		return nil
	})
	defer restore()

	reportCalls := 0
	client := fakeControllerClient{
		assignments: []controllermeta.GroupAssignment{assignment},
		tasks:       map[uint32]controllermeta.ReconcileTask{1: task},
		reportTaskResultFn: func(_ context.Context, task controllermeta.ReconcileTask, taskErr error) error {
			reportCalls++
			if task.GroupID != 1 {
				t.Fatalf("ReportTaskResult() groupID = %d, want 1", task.GroupID)
			}
			if !errors.Is(taskErr, execErr) {
				t.Fatalf("ReportTaskResult() err = %v, want %v", taskErr, execErr)
			}
			if reportCalls == 1 {
				return ErrNotLeader
			}
			return nil
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
	if reportCalls != 2 {
		t.Fatalf("ReportTaskResult() calls = %d, want 2", reportCalls)
	}
}

func TestGroupAgentRetriesTaskResultReportAfterTransientControllerTimeout(t *testing.T) {
	harness := newStandaloneAgentTestCluster(t)
	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1},
		ConfigEpoch:  1,
	}
	task := controllermeta.ReconcileTask{
		GroupID:   1,
		Kind:      controllermeta.TaskKindBootstrap,
		Step:      controllermeta.TaskStepAddLearner,
		Status:    controllermeta.TaskStatusPending,
		NextRunAt: time.Now(),
	}
	execErr := errors.New("injected execution failure")
	restore := SetManagedGroupExecutionTestHook(func(groupID uint32, got controllermeta.ReconcileTask) error {
		if groupID == 1 && got.Kind == controllermeta.TaskKindBootstrap {
			return execErr
		}
		return nil
	})
	defer restore()

	reportCalls := 0
	client := fakeControllerClient{
		assignments: []controllermeta.GroupAssignment{assignment},
		tasks:       map[uint32]controllermeta.ReconcileTask{1: task},
		reportTaskResultFn: func(_ context.Context, task controllermeta.ReconcileTask, taskErr error) error {
			reportCalls++
			if task.GroupID != 1 {
				t.Fatalf("ReportTaskResult() groupID = %d, want 1", task.GroupID)
			}
			if !errors.Is(taskErr, execErr) {
				t.Fatalf("ReportTaskResult() err = %v, want %v", taskErr, execErr)
			}
			if reportCalls == 1 {
				return context.DeadlineExceeded
			}
			return nil
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
	if reportCalls != 2 {
		t.Fatalf("ReportTaskResult() calls = %d, want 2", reportCalls)
	}
}

func TestGroupAgentApplyAssignmentsRetriesTransientGetTaskTimeoutWithoutLocalControllerMeta(t *testing.T) {
	harness := newStandaloneAgentTestCluster(t)
	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1},
		ConfigEpoch:  1,
	}
	task := controllermeta.ReconcileTask{
		GroupID:   1,
		Kind:      controllermeta.TaskKindBootstrap,
		Step:      controllermeta.TaskStepAddLearner,
		Status:    controllermeta.TaskStatusPending,
		NextRunAt: time.Now(),
	}

	getTaskCalls := 0
	client := fakeControllerClient{
		assignments: []controllermeta.GroupAssignment{assignment},
		getTaskFn: func(_ context.Context, groupID uint32) (controllermeta.ReconcileTask, error) {
			getTaskCalls++
			if groupID != 1 {
				t.Fatalf("GetTask() groupID = %d, want 1", groupID)
			}
			if getTaskCalls == 1 {
				return controllermeta.ReconcileTask{}, context.DeadlineExceeded
			}
			return task, nil
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
	if getTaskCalls < 2 {
		t.Fatalf("GetTask() calls = %d, want >= 2", getTaskCalls)
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

func TestGroupAgentDoesNotReexecuteTaskWhileResultReportIsPending(t *testing.T) {
	harness := newStandaloneAgentTestCluster(t)
	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1},
		ConfigEpoch:  1,
	}
	task := controllermeta.ReconcileTask{
		GroupID:   1,
		Kind:      controllermeta.TaskKindBootstrap,
		Step:      controllermeta.TaskStepAddLearner,
		Status:    controllermeta.TaskStatusPending,
		NextRunAt: time.Now(),
	}
	execCalls := 0
	restore := SetManagedGroupExecutionTestHook(func(groupID uint32, got controllermeta.ReconcileTask) error {
		if groupID == 1 && got.Kind == controllermeta.TaskKindBootstrap {
			execCalls++
		}
		return nil
	})
	defer restore()

	allowReport := false
	reportCalls := 0
	client := fakeControllerClient{
		assignments: []controllermeta.GroupAssignment{assignment},
		tasks:       map[uint32]controllermeta.ReconcileTask{1: task},
		reportTaskResultFn: func(_ context.Context, task controllermeta.ReconcileTask, _ error) error {
			reportCalls++
			if task.GroupID != 1 {
				t.Fatalf("ReportTaskResult() groupID = %d, want 1", task.GroupID)
			}
			if !allowReport {
				return ErrNotLeader
			}
			return nil
		},
	}
	harness.cluster.assignments.SetAssignments(client.assignments)
	harness.cluster.agent = &groupAgent{
		cluster: harness.cluster,
		client:  client,
		cache:   harness.cluster.assignments,
	}

	firstCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := harness.cluster.agent.ApplyAssignments(firstCtx); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("ApplyAssignments() first error = %v, want %v", err, ErrNotLeader)
	}
	if execCalls != 1 {
		t.Fatalf("exec calls after first ApplyAssignments() = %d, want 1", execCalls)
	}

	allowReport = true
	if err := harness.cluster.agent.ApplyAssignments(context.Background()); err != nil {
		t.Fatalf("ApplyAssignments() second error = %v", err)
	}
	if execCalls != 1 {
		t.Fatalf("exec calls after second ApplyAssignments() = %d, want 1", execCalls)
	}
	if reportCalls < 2 {
		t.Fatalf("ReportTaskResult() calls = %d, want >= 2", reportCalls)
	}
}

func TestGroupAgentRetriesPendingTaskReportWithoutRefreshingTask(t *testing.T) {
	harness := newStandaloneAgentTestCluster(t)
	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1},
		ConfigEpoch:  1,
	}
	task := controllermeta.ReconcileTask{
		GroupID:   1,
		Kind:      controllermeta.TaskKindBootstrap,
		Step:      controllermeta.TaskStepAddLearner,
		Status:    controllermeta.TaskStatusPending,
		NextRunAt: time.Now(),
	}
	execErr := errors.New("injected execution failure")
	restore := SetManagedGroupExecutionTestHook(func(groupID uint32, got controllermeta.ReconcileTask) error {
		if groupID == 1 && got.Kind == controllermeta.TaskKindBootstrap {
			return execErr
		}
		return nil
	})
	defer restore()

	getTaskCalls := 0
	reportCalls := 0
	client := fakeControllerClient{
		assignments: []controllermeta.GroupAssignment{assignment},
		getTaskFn: func(_ context.Context, groupID uint32) (controllermeta.ReconcileTask, error) {
			getTaskCalls++
			if groupID != 1 {
				t.Fatalf("GetTask() groupID = %d, want 1", groupID)
			}
			if getTaskCalls <= 2 {
				return task, nil
			}
			return controllermeta.ReconcileTask{}, context.DeadlineExceeded
		},
		reportTaskResultFn: func(_ context.Context, got controllermeta.ReconcileTask, taskErr error) error {
			reportCalls++
			if got.GroupID != 1 {
				t.Fatalf("ReportTaskResult() groupID = %d, want 1", got.GroupID)
			}
			if got.Attempt != task.Attempt {
				t.Fatalf("ReportTaskResult() attempt = %d, want %d", got.Attempt, task.Attempt)
			}
			if !errors.Is(taskErr, execErr) {
				t.Fatalf("ReportTaskResult() err = %v, want %v", taskErr, execErr)
			}
			if reportCalls == 1 {
				return context.DeadlineExceeded
			}
			return nil
		},
	}
	harness.cluster.assignments.SetAssignments(client.assignments)
	harness.cluster.agent = &groupAgent{
		cluster: harness.cluster,
		client:  client,
		cache:   harness.cluster.assignments,
	}

	firstCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := harness.cluster.agent.ApplyAssignments(firstCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ApplyAssignments() first error = %v, want %v", err, context.DeadlineExceeded)
	}
	if reportCalls != 1 {
		t.Fatalf("ReportTaskResult() calls after first ApplyAssignments() = %d, want 1", reportCalls)
	}

	if err := harness.cluster.agent.ApplyAssignments(context.Background()); err != nil {
		t.Fatalf("ApplyAssignments() second error = %v", err)
	}
	if reportCalls != 2 {
		t.Fatalf("ReportTaskResult() calls after second ApplyAssignments() = %d, want 2", reportCalls)
	}
	if getTaskCalls < 2 {
		t.Fatalf("GetTask() calls = %d, want >= 2", getTaskCalls)
	}
}

func TestGroupAgentExecutesKnownTaskWhenFreshConfirmationTimesOut(t *testing.T) {
	harness := newStandaloneAgentTestCluster(t)
	assignment := controllermeta.GroupAssignment{
		GroupID:      1,
		DesiredPeers: []uint64{1},
		ConfigEpoch:  1,
	}
	task := controllermeta.ReconcileTask{
		GroupID:   1,
		Kind:      controllermeta.TaskKindBootstrap,
		Step:      controllermeta.TaskStepAddLearner,
		Status:    controllermeta.TaskStatusPending,
		NextRunAt: time.Now(),
	}
	execErr := errors.New("injected execution failure")
	execCalls := 0
	restore := SetManagedGroupExecutionTestHook(func(groupID uint32, got controllermeta.ReconcileTask) error {
		if groupID == 1 && got.Kind == controllermeta.TaskKindBootstrap {
			execCalls++
			return execErr
		}
		return nil
	})
	defer restore()

	getTaskCalls := 0
	reportCalls := 0
	client := fakeControllerClient{
		assignments: []controllermeta.GroupAssignment{assignment},
		getTaskFn: func(_ context.Context, groupID uint32) (controllermeta.ReconcileTask, error) {
			getTaskCalls++
			if groupID != 1 {
				t.Fatalf("GetTask() groupID = %d, want 1", groupID)
			}
			if getTaskCalls == 1 {
				return task, nil
			}
			return controllermeta.ReconcileTask{}, context.DeadlineExceeded
		},
		reportTaskResultFn: func(_ context.Context, got controllermeta.ReconcileTask, taskErr error) error {
			reportCalls++
			if got.GroupID != 1 {
				t.Fatalf("ReportTaskResult() groupID = %d, want 1", got.GroupID)
			}
			if got.Attempt != task.Attempt {
				t.Fatalf("ReportTaskResult() attempt = %d, want %d", got.Attempt, task.Attempt)
			}
			if !errors.Is(taskErr, execErr) {
				t.Fatalf("ReportTaskResult() err = %v, want %v", taskErr, execErr)
			}
			return nil
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
	if getTaskCalls < 2 {
		t.Fatalf("GetTask() calls = %d, want >= 2", getTaskCalls)
	}
	if execCalls != 1 {
		t.Fatalf("execution calls = %d, want 1", execCalls)
	}
	if reportCalls != 1 {
		t.Fatalf("ReportTaskResult() calls = %d, want 1", reportCalls)
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

func TestWaitForManagedGroupCatchUpAllowsSlowLearnerCatchUp(t *testing.T) {
	restoreLeader := setManagedGroupLeaderTestHook(func(_ *Cluster, groupID multiraft.GroupID) (multiraft.NodeID, error, bool) {
		if groupID != 1 {
			return 0, nil, false
		}
		return 1, nil, true
	})
	defer restoreLeader()

	start := time.Now()
	restore := setManagedGroupStatusTestHook(func(_ *Cluster, nodeID multiraft.NodeID, groupID multiraft.GroupID) (managedGroupStatus, error, bool) {
		if groupID != 1 {
			return managedGroupStatus{}, nil, false
		}
		switch nodeID {
		case 1:
			return managedGroupStatus{LeaderID: 1, CommitIndex: 9, AppliedIndex: 9}, nil, true
		case 4:
			applied := uint64(3)
			if time.Since(start) >= 2300*time.Millisecond {
				applied = 9
			}
			return managedGroupStatus{LeaderID: 1, CommitIndex: 9, AppliedIndex: applied}, nil, true
		default:
			return managedGroupStatus{}, ErrGroupNotFound, true
		}
	})
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 3500*time.Millisecond)
	defer cancel()

	if err := (&Cluster{}).waitForManagedGroupCatchUp(ctx, 1, 4); err != nil {
		t.Fatalf("waitForManagedGroupCatchUp() error = %v, want nil", err)
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
