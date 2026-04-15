package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
	"go.etcd.io/raft/v3/raftpb"
)

func TestObserverHooksOnForwardPropose(t *testing.T) {
	var (
		gotSlot     uint32
		gotAttempts int
		gotErr      error
		calls       int
	)
	cluster := newObserverTestCluster(t, ObserverHooks{
		OnForwardPropose: func(slotID uint32, attempts int, dur time.Duration, err error) {
			calls++
			gotSlot = slotID
			gotAttempts = attempts
			gotErr = err
			if dur <= 0 {
				t.Fatalf("OnForwardPropose() duration = %v, want > 0", dur)
			}
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := cluster.ensureManagedSlotLocal(ctx, 1, []uint64{1}, false, true); err != nil {
		t.Fatalf("ensureManagedSlotLocal() error = %v", err)
	}
	if err := cluster.waitForManagedSlotLeader(ctx, 1); err != nil {
		t.Fatalf("waitForManagedSlotLeader() error = %v", err)
	}
	if err := cluster.Propose(ctx, 1, []byte("observer-propose")); err != nil {
		t.Fatalf("Propose() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("OnForwardPropose() calls = %d, want 1", calls)
	}
	if gotSlot != 1 {
		t.Fatalf("OnForwardPropose() slotID = %d, want 1", gotSlot)
	}
	if gotAttempts < 1 {
		t.Fatalf("OnForwardPropose() attempts = %d, want >= 1", gotAttempts)
	}
	if gotErr != nil {
		t.Fatalf("OnForwardPropose() err = %v, want nil", gotErr)
	}
}

func TestObserverHooksOnControllerCall(t *testing.T) {
	server := transport.NewServer()
	mux := transport.NewRPCMux()
	mux.Handle(rpcServiceController, func(_ context.Context, body []byte) ([]byte, error) {
		req, err := decodeControllerRequest(body)
		if err != nil {
			t.Fatalf("decodeControllerRequest() error = %v", err)
		}
		if req.Kind != controllerRPCListNodes {
			t.Fatalf("request kind = %q, want %q", req.Kind, controllerRPCListNodes)
		}
		return encodeControllerResponse(req.Kind, controllerRPCResponse{
			Nodes: []controllermeta.ClusterNode{{NodeID: 2, Addr: "n2"}},
		})
	})
	server.HandleRPCMux(mux)
	if err := server.Start("127.0.0.1:0"); err != nil {
		t.Fatalf("server.Start() error = %v", err)
	}
	defer server.Stop()

	discovery := &controllerClientTestDiscovery{
		addrs: map[uint64]string{2: server.Listener().Addr().String()},
	}
	pool := transport.NewPool(discovery, 1, 50*time.Millisecond)
	defer pool.Close()

	client := transport.NewClient(pool)
	defer client.Stop()

	var (
		gotKind string
		gotErr  error
		calls   int
	)
	cluster := newObserverTestCluster(t, ObserverHooks{
		OnControllerCall: func(kind string, dur time.Duration, err error) {
			calls++
			gotKind = kind
			gotErr = err
			if dur <= 0 {
				t.Fatalf("OnControllerCall() duration = %v, want > 0", dur)
			}
		},
	})
	cluster.fwdClient = client

	controllerClient := newControllerClient(cluster, []NodeConfig{{NodeID: 2}}, nil)
	nodes, err := controllerClient.ListNodes(context.Background())
	if err != nil {
		t.Fatalf("ListNodes() error = %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeID != 2 {
		t.Fatalf("ListNodes() = %+v", nodes)
	}
	if calls != 1 {
		t.Fatalf("OnControllerCall() calls = %d, want 1", calls)
	}
	if gotKind != controllerRPCListNodes {
		t.Fatalf("OnControllerCall() kind = %q, want %q", gotKind, controllerRPCListNodes)
	}
	if gotErr != nil {
		t.Fatalf("OnControllerCall() err = %v, want nil", gotErr)
	}
}

func TestObserverHooksOnReconcileStep(t *testing.T) {
	sentinel := errors.New("reconcile step failed")
	var (
		gotSlot uint32
		gotStep string
		gotErr  error
		calls   int
	)
	cluster := newObserverTestCluster(t, ObserverHooks{
		OnReconcileStep: func(slotID uint32, step string, dur time.Duration, err error) {
			calls++
			gotSlot = slotID
			gotStep = step
			gotErr = err
			if dur <= 0 {
				t.Fatalf("OnReconcileStep() duration = %v, want > 0", dur)
			}
		},
	})
	restore := cluster.SetManagedSlotExecutionTestHook(func(slotID uint32, task controllermeta.ReconcileTask) error {
		if slotID != 1 || task.Step != controllermeta.TaskStepAddLearner {
			t.Fatalf("execution hook got slot=%d step=%d", slotID, task.Step)
		}
		return sentinel
	})
	defer restore()

	err := cluster.executeReconcileTask(context.Background(), assignmentTaskState{
		assignment: controllermeta.SlotAssignment{SlotID: 1, DesiredPeers: []uint64{1}},
		task: controllermeta.ReconcileTask{
			SlotID: 1,
			Kind:   controllermeta.TaskKindRepair,
			Step:   controllermeta.TaskStepAddLearner,
		},
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("executeReconcileTask() error = %v, want %v", err, sentinel)
	}
	if calls != 1 {
		t.Fatalf("OnReconcileStep() calls = %d, want 1", calls)
	}
	if gotSlot != 1 {
		t.Fatalf("OnReconcileStep() slotID = %d, want 1", gotSlot)
	}
	if gotStep != "add_learner" {
		t.Fatalf("OnReconcileStep() step = %q, want %q", gotStep, "add_learner")
	}
	if !errors.Is(gotErr, sentinel) {
		t.Fatalf("OnReconcileStep() err = %v, want %v", gotErr, sentinel)
	}
}

func TestObserverHooksOnSlotEnsure(t *testing.T) {
	var (
		gotSlot   uint32
		gotAction string
		gotErr    error
		calls     int
	)
	cluster := newObserverTestCluster(t, ObserverHooks{
		OnSlotEnsure: func(slotID uint32, action string, err error) {
			calls++
			gotSlot = slotID
			gotAction = action
			gotErr = err
		},
	})

	if err := cluster.ensureManagedSlotLocal(context.Background(), 1, []uint64{1}, false, true); err != nil {
		t.Fatalf("ensureManagedSlotLocal() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("OnSlotEnsure() calls = %d, want 1", calls)
	}
	if gotSlot != 1 {
		t.Fatalf("OnSlotEnsure() slotID = %d, want 1", gotSlot)
	}
	if gotAction != "bootstrap" {
		t.Fatalf("OnSlotEnsure() action = %q, want %q", gotAction, "bootstrap")
	}
	if gotErr != nil {
		t.Fatalf("OnSlotEnsure() err = %v, want nil", gotErr)
	}
}

func TestObserverHooksOnSlotOpen(t *testing.T) {
	var (
		gotSlot   uint32
		gotAction string
		gotErr    error
		calls     int
	)
	cluster := newObserverTestCluster(t, ObserverHooks{
		OnSlotEnsure: func(slotID uint32, action string, err error) {
			calls++
			gotSlot = slotID
			gotAction = action
			gotErr = err
		},
	})
	cluster.cfg.NewStorage = func(multiraft.SlotID) (multiraft.Storage, error) {
		return &observerTestStorage{
			state: multiraft.BootstrapState{
				HardState: raftpb.HardState{Term: 1},
				ConfState: raftpb.ConfState{Voters: []uint64{1}},
			},
		}, nil
	}

	if err := cluster.openOrBootstrapSlot(context.Background(), SlotConfig{SlotID: 1, Peers: []multiraft.NodeID{1}}); err != nil {
		t.Fatalf("openOrBootstrapSlot() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("OnSlotEnsure() calls = %d, want 1", calls)
	}
	if gotSlot != 1 {
		t.Fatalf("OnSlotEnsure() slotID = %d, want 1", gotSlot)
	}
	if gotAction != "open" {
		t.Fatalf("OnSlotEnsure() action = %q, want %q", gotAction, "open")
	}
	if gotErr != nil {
		t.Fatalf("OnSlotEnsure() err = %v, want nil", gotErr)
	}
}

func TestObserverHooksOnSlotClose(t *testing.T) {
	var (
		gotSlot   uint32
		gotAction string
		gotErr    error
		calls     int
	)
	cluster := newObserverTestCluster(t, ObserverHooks{
		OnSlotEnsure: func(slotID uint32, action string, err error) {
			calls++
			gotSlot = slotID
			gotAction = action
			gotErr = err
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := cluster.ensureManagedSlotLocal(ctx, 1, []uint64{1}, false, true); err != nil {
		t.Fatalf("ensureManagedSlotLocal() error = %v", err)
	}
	if err := cluster.waitForManagedSlotLeader(ctx, 1); err != nil {
		t.Fatalf("waitForManagedSlotLeader() error = %v", err)
	}

	cluster.assignments.SetAssignments([]controllermeta.SlotAssignment{
		{SlotID: 1, DesiredPeers: []uint64{2}},
	})
	agent := &slotAgent{
		cluster: cluster,
		client:  fakeControllerClient{},
		cache:   cluster.assignments,
	}
	calls = 0

	if err := agent.ApplyAssignments(ctx); err != nil {
		t.Fatalf("ApplyAssignments() error = %v", err)
	}
	if _, err := cluster.runtime.Status(1); !errors.Is(err, multiraft.ErrSlotNotFound) {
		t.Fatalf("runtime.Status() error = %v, want %v", err, multiraft.ErrSlotNotFound)
	}
	if calls != 1 {
		t.Fatalf("OnSlotEnsure() calls = %d, want 1", calls)
	}
	if gotSlot != 1 {
		t.Fatalf("OnSlotEnsure() slotID = %d, want 1", gotSlot)
	}
	if gotAction != "close" {
		t.Fatalf("OnSlotEnsure() action = %q, want %q", gotAction, "close")
	}
	if gotErr != nil {
		t.Fatalf("OnSlotEnsure() err = %v, want nil", gotErr)
	}
}

func TestObserverHooksOnLeaderChange(t *testing.T) {
	var (
		gotSlot uint32
		gotFrom multiraft.NodeID
		gotTo   multiraft.NodeID
		calls   int
	)
	cluster := newObserverTestCluster(t, ObserverHooks{
		OnLeaderChange: func(slotID uint32, from, to multiraft.NodeID) {
			calls++
			gotSlot = slotID
			gotFrom = from
			gotTo = to
		},
	})

	attempts := 0
	restore := cluster.setManagedSlotLeaderTestHook(func(_ *Cluster, slotID multiraft.SlotID) (multiraft.NodeID, error, bool) {
		if slotID != 1 {
			return 0, nil, false
		}
		attempts++
		if attempts < 2 {
			return 0, nil, true
		}
		return 2, nil, true
	})
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	if err := cluster.waitForManagedSlotLeader(ctx, 1); err != nil {
		t.Fatalf("waitForManagedSlotLeader() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("OnLeaderChange() calls = %d, want 1", calls)
	}
	if gotSlot != 1 {
		t.Fatalf("OnLeaderChange() slotID = %d, want 1", gotSlot)
	}
	if gotFrom != 0 || gotTo != 2 {
		t.Fatalf("OnLeaderChange() from=%d to=%d, want 0->2", gotFrom, gotTo)
	}
}

func TestObserverHooksOnLeaderMoveTransition(t *testing.T) {
	var (
		gotSlot uint32
		gotFrom multiraft.NodeID
		gotTo   multiraft.NodeID
		calls   int
	)
	hooks := ObserverHooks{
		OnLeaderChange: func(slotID uint32, from, to multiraft.NodeID) {
			calls++
			gotSlot = slotID
			gotFrom = from
			gotTo = to
		},
	}
	cluster := &Cluster{
		cfg: Config{
			Observer: hooks,
			Timeouts: Timeouts{
				ManagedSlotLeaderMove: 10 * time.Millisecond,
			},
		},
		obs: hooks,
	}

	restore := cluster.setManagedSlotLeaderTestHook(func(_ *Cluster, slotID multiraft.SlotID) (multiraft.NodeID, error, bool) {
		if slotID != 1 {
			return 0, nil, false
		}
		return 2, nil, true
	})
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := cluster.ensureLeaderMovedOffSource(ctx, 1, 1, 2); err != nil {
		t.Fatalf("ensureLeaderMovedOffSource() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("OnLeaderChange() calls = %d, want 1", calls)
	}
	if gotSlot != 1 {
		t.Fatalf("OnLeaderChange() slotID = %d, want 1", gotSlot)
	}
	if gotFrom != 1 || gotTo != 2 {
		t.Fatalf("OnLeaderChange() from=%d to=%d, want 1->2", gotFrom, gotTo)
	}
}

func TestObserverHooksOnLeaderMoveSkipsUnknownLeader(t *testing.T) {
	var (
		gotSlot uint32
		gotFrom multiraft.NodeID
		gotTo   multiraft.NodeID
		calls   int
	)
	hooks := ObserverHooks{
		OnLeaderChange: func(slotID uint32, from, to multiraft.NodeID) {
			calls++
			gotSlot = slotID
			gotFrom = from
			gotTo = to
		},
	}
	cluster := &Cluster{
		cfg: Config{
			Observer: hooks,
			Timeouts: Timeouts{
				ManagedSlotLeaderMove: 20 * time.Millisecond,
			},
		},
		obs: hooks,
	}

	attempts := 0
	restore := cluster.setManagedSlotLeaderTestHook(func(_ *Cluster, slotID multiraft.SlotID) (multiraft.NodeID, error, bool) {
		if slotID != 1 {
			return 0, nil, false
		}
		attempts++
		if attempts == 1 {
			return 0, nil, true
		}
		return 2, nil, true
	})
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := cluster.ensureLeaderMovedOffSource(ctx, 1, 1, 2); err != nil {
		t.Fatalf("ensureLeaderMovedOffSource() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("OnLeaderChange() calls = %d, want 1", calls)
	}
	if gotSlot != 1 {
		t.Fatalf("OnLeaderChange() slotID = %d, want 1", gotSlot)
	}
	if gotFrom != 1 || gotTo != 2 {
		t.Fatalf("OnLeaderChange() from=%d to=%d, want 1->2", gotFrom, gotTo)
	}
}

type observerTestTransport struct{}

func (observerTestTransport) Send(context.Context, []multiraft.Envelope) error {
	return nil
}

type observerTestStorage struct {
	state   multiraft.BootstrapState
	applied uint64
}

func (s *observerTestStorage) InitialState(context.Context) (multiraft.BootstrapState, error) {
	return s.state, nil
}

func (s *observerTestStorage) Entries(context.Context, uint64, uint64, uint64) ([]raftpb.Entry, error) {
	return nil, nil
}

func (s *observerTestStorage) Term(context.Context, uint64) (uint64, error) {
	return 0, nil
}

func (s *observerTestStorage) FirstIndex(context.Context) (uint64, error) {
	return 0, nil
}

func (s *observerTestStorage) LastIndex(context.Context) (uint64, error) {
	return 0, nil
}

func (s *observerTestStorage) Snapshot(context.Context) (raftpb.Snapshot, error) {
	return raftpb.Snapshot{}, nil
}

func (s *observerTestStorage) Save(_ context.Context, st multiraft.PersistentState) error {
	if st.HardState != nil {
		s.state.HardState = *st.HardState
	}
	if st.Snapshot != nil {
		s.state.HardState.Commit = st.Snapshot.Metadata.Index
	}
	return nil
}

func (s *observerTestStorage) MarkApplied(_ context.Context, index uint64) error {
	s.applied = index
	s.state.AppliedIndex = index
	return nil
}

type observerTestStateMachine struct{}

func (observerTestStateMachine) Apply(_ context.Context, cmd multiraft.Command) ([]byte, error) {
	return append([]byte(nil), cmd.Data...), nil
}

func (observerTestStateMachine) Restore(context.Context, multiraft.Snapshot) error {
	return nil
}

func (observerTestStateMachine) Snapshot(context.Context) (multiraft.Snapshot, error) {
	return multiraft.Snapshot{}, nil
}

func newObserverTestCluster(t *testing.T, hooks ObserverHooks) *Cluster {
	t.Helper()

	cluster, err := NewCluster(Config{
		NodeID:       1,
		ListenAddr:   "127.0.0.1:0",
		SlotCount:    1,
		SlotReplicaN: 1,
		Nodes: []NodeConfig{
			{NodeID: 1, Addr: "127.0.0.1:0"},
		},
		Observer: hooks,
		NewStorage: func(multiraft.SlotID) (multiraft.Storage, error) {
			return &observerTestStorage{}, nil
		},
		NewStateMachine: func(multiraft.SlotID) (multiraft.StateMachine, error) {
			return observerTestStateMachine{}, nil
		},
		Timeouts: Timeouts{
			ManagedSlotLeaderWait: 200 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("NewCluster() error = %v", err)
	}

	rt, err := multiraft.New(multiraft.Options{
		NodeID:       1,
		TickInterval: 10 * time.Millisecond,
		Workers:      1,
		Transport:    observerTestTransport{},
		Raft: multiraft.RaftOptions{
			ElectionTick:  3,
			HeartbeatTick: 1,
		},
	})
	if err != nil {
		t.Fatalf("multiraft.New() error = %v", err)
	}
	t.Cleanup(func() {
		_ = rt.Close()
	})

	cluster.runtime = rt
	cluster.router = NewRouter(cluster.cfg.SlotCount, cluster.cfg.NodeID, rt)
	return cluster
}
