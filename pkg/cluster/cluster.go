package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
	controllerraft "github.com/WuKongIM/WuKongIM/pkg/controller/raft"
	raftstorage "github.com/WuKongIM/WuKongIM/pkg/raftlog"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	raft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type transportResources struct {
	transportLayer *transportLayer
	server         *transport.Server
	rpcMux         *transport.RPCMux
	raftPool       *transport.Pool
	rpcPool        *transport.Pool
	raftClient     *transport.Client
	fwdClient      *transport.Client
	discovery      *StaticDiscovery
}

type controllerResources struct {
	controllerHost              *controllerHost
	controllerMeta              *controllermeta.Store
	controllerRaftDB            *raftstorage.DB
	controllerSM                *slotcontroller.StateMachine
	controller                  *controllerraft.Service
	controllerClient            controllerAPI
	controllerLeaderWaitTimeout time.Duration
}

type managedSlotResources struct {
	managedSlotHooks managedSlotHooks
	slotMgr          *slotManager
	slotExecutor     *slotExecutor
}

type Cluster struct {
	cfg    Config
	logger wklog.Logger
	obs    ObserverHooks
	transportResources
	runtime *multiraft.Runtime
	router  *Router
	controllerResources
	agent       *slotAgent
	assignments *assignmentCache
	runState    *runtimeState
	observer    *observerLoop
	managedSlotResources
	stopped atomic.Bool
}

func NewCluster(cfg Config) (*Cluster, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cluster := &Cluster{
		cfg:    cfg,
		logger: defaultLogger(cfg.Logger),
		obs:    cfg.Observer,
		transportResources: transportResources{
			rpcMux: transport.NewRPCMux(),
		},
		router:      NewRouter(cfg.SlotCount, cfg.NodeID, nil),
		assignments: newAssignmentCache(),
		runState:    newRuntimeState(),
	}
	cluster.slotMgr = newSlotManager(cluster)
	cluster.slotExecutor = newSlotExecutor(cluster)
	return cluster, nil
}

func defaultLogger(logger wklog.Logger) wklog.Logger {
	if logger == nil {
		return wklog.NewNop()
	}
	return logger
}

func (c *Cluster) Start() error {
	if err := c.startTransportLayer(); err != nil {
		return err
	}
	if err := c.startControllerRaftIfLocalPeer(); err != nil {
		c.Stop()
		return err
	}
	if err := c.startMultiraftRuntime(); err != nil {
		c.Stop()
		return err
	}
	c.startControllerClient()
	c.startObservationLoop()
	if err := c.seedLegacySlotsIfConfigured(); err != nil {
		c.Stop()
		return err
	}
	return nil
}

func (c *Cluster) startTransportLayer() error {
	layer := newTransportLayer(c.cfg, NewStaticDiscovery(c.cfg.Nodes), c.rpcMux)
	if err := layer.Start(c.cfg.ListenAddr, c.handleRaftMessage, c.handleForwardRPC, c.handleControllerRPC, c.handleManagedSlotRPC); err != nil {
		return err
	}

	c.transportLayer = layer
	c.server = layer.server
	c.rpcMux = layer.rpcMux
	c.raftPool = layer.raftPool
	c.rpcPool = layer.rpcPool
	c.raftClient = layer.raftClient
	c.fwdClient = layer.fwdClient
	c.discovery = layer.discovery
	return nil
}

func (c *Cluster) startServer() error {
	c.server = transport.NewServer()
	c.server.Handle(msgTypeRaft, c.handleRaftMessage)
	c.rpcMux.Handle(rpcServiceForward, c.handleForwardRPC)
	c.rpcMux.Handle(rpcServiceController, c.handleControllerRPC)
	c.rpcMux.Handle(rpcServiceManagedSlot, c.handleManagedSlotRPC)
	c.server.HandleRPCMux(c.rpcMux)
	if err := c.server.Start(c.cfg.ListenAddr); err != nil {
		return fmt.Errorf("start server: %w", err)
	}
	return nil
}

func (c *Cluster) startPools() {
	c.raftPool = transport.NewPool(transport.PoolConfig{
		Discovery:   c.discovery,
		Size:        c.cfg.PoolSize,
		DialTimeout: c.cfg.DialTimeout,
		QueueSizes:  [3]int{2048, 0, 0},
		DefaultPri:  transport.PriorityRaft,
	})
	c.rpcPool = transport.NewPool(transport.PoolConfig{
		Discovery:   c.discovery,
		Size:        c.cfg.PoolSize,
		DialTimeout: c.cfg.DialTimeout,
		QueueSizes:  [3]int{0, 1024, 256},
		DefaultPri:  transport.PriorityRPC,
	})
	c.raftClient = transport.NewClient(c.raftPool)
	c.fwdClient = transport.NewClient(c.rpcPool)
}

func (c *Cluster) startControllerRaftIfLocalPeer() error {
	if !c.cfg.ControllerEnabled() {
		return nil
	}
	if !c.cfg.HasLocalControllerPeer() {
		return nil
	}

	host, err := newControllerHost(c.cfg, c.transportLayer)
	if err != nil {
		return err
	}
	if err := host.Start(context.Background()); err != nil {
		host.Stop()
		return fmt.Errorf("start controller raft: %w", err)
	}

	c.controllerHost = host
	c.controllerMeta = host.meta
	c.controllerRaftDB = host.raftDB
	c.controllerSM = host.sm
	c.controller = host.service
	return nil
}

func (c *Cluster) startMultiraftRuntime() error {
	var err error
	c.runtime, err = multiraft.New(multiraft.Options{
		NodeID:       c.cfg.NodeID,
		TickInterval: c.cfg.TickInterval,
		Workers:      c.cfg.RaftWorkers,
		Transport:    &raftTransport{client: c.raftClient},
		Raft: multiraft.RaftOptions{
			ElectionTick:  c.cfg.ElectionTick,
			HeartbeatTick: c.cfg.HeartbeatTick,
		},
	})
	if err != nil {
		return fmt.Errorf("create runtime: %w", err)
	}

	if c.router == nil {
		c.router = NewRouter(c.cfg.SlotCount, c.cfg.NodeID, c.runtime)
	} else {
		c.router.runtime = c.runtime
	}
	return nil
}

func (c *Cluster) startControllerClient() {
	if !c.cfg.ControllerEnabled() {
		return
	}
	client := newControllerClient(c, c.cfg.DerivedControllerNodes(), c.assignments)
	c.controllerClient = client
	c.agent = &slotAgent{
		cluster: c,
		client:  client,
		cache:   c.assignments,
	}
}

func (c *Cluster) startObservationLoop() {
	if c.controllerClient == nil {
		return
	}
	c.observer = newObserverLoop(c.controllerObservationInterval(), func(ctx context.Context) {
		c.observeOnce(ctx)
		c.controllerTickOnce(ctx)
	})
	c.observer.Start(context.Background())
}

func (c *Cluster) seedLegacySlotsIfConfigured() error {
	if c.cfg.ControllerEnabled() {
		return nil
	}
	ctx := context.Background()
	for _, g := range c.cfg.Slots {
		if err := c.openOrBootstrapSlot(ctx, g); err != nil {
			return fmt.Errorf("open slot %d: %w", g.SlotID, err)
		}
	}
	return nil
}

func (c *Cluster) openOrBootstrapSlot(ctx context.Context, g SlotConfig) error {
	storage, err := c.cfg.NewStorage(g.SlotID)
	if err != nil {
		return fmt.Errorf("create storage for slot %d: %w", g.SlotID, err)
	}
	sm, err := c.cfg.NewStateMachine(g.SlotID)
	if err != nil {
		return fmt.Errorf("create state machine for slot %d: %w", g.SlotID, err)
	}
	opts := multiraft.SlotOptions{
		ID:           g.SlotID,
		Storage:      storage,
		StateMachine: sm,
	}

	initialState, err := storage.InitialState(ctx)
	if err != nil {
		return err
	}
	if !raft.IsEmptyHardState(initialState.HardState) {
		c.setRuntimePeers(g.SlotID, nodeIDsFromUint64s(initialState.ConfState.Voters))
		if err := c.runtime.OpenSlot(ctx, opts); err != nil {
			c.deleteRuntimePeers(g.SlotID)
			if hook := c.obs.OnSlotEnsure; hook != nil {
				hook(uint32(g.SlotID), "open", err)
			}
			return err
		}
		if hook := c.obs.OnSlotEnsure; hook != nil {
			hook(uint32(g.SlotID), "open", nil)
		}
		return nil
	}
	c.setRuntimePeers(g.SlotID, g.Peers)
	if err := c.runtime.BootstrapSlot(ctx, multiraft.BootstrapSlotRequest{
		Slot:   opts,
		Voters: g.Peers,
	}); err != nil {
		c.deleteRuntimePeers(g.SlotID)
		if hook := c.obs.OnSlotEnsure; hook != nil {
			hook(uint32(g.SlotID), "bootstrap", err)
		}
		return err
	}
	if hook := c.obs.OnSlotEnsure; hook != nil {
		hook(uint32(g.SlotID), "bootstrap", nil)
	}
	return nil
}

func (c *Cluster) Stop() {
	c.stopped.Store(true)
	if c.observer != nil {
		c.observer.Stop()
		c.observer = nil
	}

	if c.runtime != nil {
		_ = c.runtime.Close()
	}
	if c.controllerHost != nil {
		c.controllerHost.Stop()
		c.controllerHost = nil
	} else {
		if c.controller != nil {
			_ = c.controller.Stop()
		}
		if c.controllerRaftDB != nil {
			_ = c.controllerRaftDB.Close()
		}
		if c.controllerMeta != nil {
			_ = c.controllerMeta.Close()
		}
	}
	if c.transportLayer != nil {
		c.transportLayer.Stop()
		c.transportLayer = nil
	} else {
		if c.fwdClient != nil {
			c.fwdClient.Stop()
		}
		if c.raftClient != nil {
			c.raftClient.Stop()
		}
		if c.raftPool != nil {
			c.raftPool.Close()
		}
		if c.rpcPool != nil {
			c.rpcPool.Close()
		}
		if c.server != nil {
			c.server.Stop()
		}
	}
}

func (c *Cluster) observeOnce(ctx context.Context) {
	if c.agent == nil || c.runtime == nil || c.stopped.Load() {
		return
	}
	_ = c.agent.HeartbeatOnce(ctx)
	assignCtx, cancel := c.withControllerTimeout(ctx)
	err := c.agent.SyncAssignments(assignCtx)
	cancel()
	shouldApply := err == nil
	if !shouldApply && controllerReadFallbackAllowed(err) && len(c.ListCachedAssignments()) > 0 {
		shouldApply = true
	}
	if shouldApply {
		_ = c.agent.ApplyAssignments(ctx)
	}
}

func (c *Cluster) controllerTickOnce(ctx context.Context) {
	if c.controller == nil || c.controllerMeta == nil || c.stopped.Load() {
		return
	}
	if c.controller.LeaderID() != uint64(c.cfg.NodeID) {
		return
	}

	tickCtx, cancel := c.withControllerTimeout(ctx)
	_ = c.controller.Propose(tickCtx, slotcontroller.Command{
		Kind:    slotcontroller.CommandKindEvaluateTimeouts,
		Advance: &slotcontroller.TaskAdvance{Now: time.Now()},
	})
	cancel()

	state, err := c.snapshotPlannerState(ctx)
	if err != nil {
		return
	}
	planner := slotcontroller.NewPlanner(slotcontroller.PlannerConfig{
		SlotCount: c.cfg.SlotCount,
		ReplicaN:  c.cfg.SlotReplicaN,
	})
	decision, err := planner.NextDecision(ctx, state)
	if err != nil || decision.SlotID == 0 || decision.Task == nil {
		return
	}
	if _, exists := state.Tasks[decision.SlotID]; exists {
		return
	}

	proposeCtx, cancel := c.withControllerTimeout(ctx)
	_ = c.controller.Propose(proposeCtx, slotcontroller.Command{
		Kind:       slotcontroller.CommandKindAssignmentTaskUpdate,
		Assignment: &decision.Assignment,
		Task:       decision.Task,
	})
	cancel()
}

func (c *Cluster) snapshotPlannerState(ctx context.Context) (slotcontroller.PlannerState, error) {
	nodes, err := c.controllerMeta.ListNodes(ctx)
	if err != nil {
		return slotcontroller.PlannerState{}, err
	}
	assignments, err := c.controllerMeta.ListAssignments(ctx)
	if err != nil {
		return slotcontroller.PlannerState{}, err
	}
	views, err := c.controllerMeta.ListRuntimeViews(ctx)
	if err != nil {
		return slotcontroller.PlannerState{}, err
	}
	tasks, err := c.controllerMeta.ListTasks(ctx)
	if err != nil {
		return slotcontroller.PlannerState{}, err
	}
	state := slotcontroller.PlannerState{
		Now:         time.Now(),
		Nodes:       make(map[uint64]controllermeta.ClusterNode, len(nodes)),
		Assignments: make(map[uint32]controllermeta.SlotAssignment, len(assignments)),
		Runtime:     make(map[uint32]controllermeta.SlotRuntimeView, len(views)),
		Tasks:       make(map[uint32]controllermeta.ReconcileTask, len(tasks)),
	}
	for _, node := range nodes {
		state.Nodes[node.NodeID] = node
	}
	for _, assignment := range assignments {
		state.Assignments[assignment.SlotID] = assignment
	}
	for _, view := range views {
		state.Runtime[view.SlotID] = view
	}
	for _, task := range tasks {
		state.Tasks[task.SlotID] = task
	}
	return state, nil
}

// handleRaftMessage is the server handler for msgTypeRaft.
func (c *Cluster) handleRaftMessage(body []byte) {
	if c.runtime == nil {
		return
	}
	slotID, data, err := decodeRaftBody(body)
	if err != nil {
		return
	}
	var msg raftpb.Message
	if err := msg.Unmarshal(data); err != nil {
		return
	}
	_ = c.runtime.Step(context.Background(), multiraft.Envelope{
		SlotID:  multiraft.SlotID(slotID),
		Message: msg,
	})
}

// Propose submits a command to the specified slot, automatically handling leader forwarding.
func (c *Cluster) Propose(ctx context.Context, slotID multiraft.SlotID, cmd []byte) error {
	start := time.Now()
	attempts := 0
	var proposeErr error
	defer func() {
		if hook := c.obs.OnForwardPropose; hook != nil {
			hook(uint32(slotID), attempts, observerElapsed(start), proposeErr)
		}
	}()

	if c.stopped.Load() {
		proposeErr = transport.ErrStopped
		return proposeErr
	}
	if c.router == nil || c.runtime == nil {
		proposeErr = ErrNotStarted
		return proposeErr
	}
	retry := Retry{
		Interval: c.forwardRetryInterval(),
		MaxWait:  c.timeoutConfig().ForwardRetryBudget,
		IsRetryable: func(err error) bool {
			return errors.Is(err, ErrNotLeader)
		},
	}
	proposeErr = retry.Do(ctx, func(attemptCtx context.Context) error {
		attempts++
		leaderID, err := c.router.LeaderOf(slotID)
		if err != nil {
			return err
		}
		if c.router.IsLocal(leaderID) {
			future, err := c.runtime.Propose(attemptCtx, slotID, cmd)
			if err != nil {
				return err
			}
			_, err = future.Wait(attemptCtx)
			return err
		}
		return c.forwardToLeader(attemptCtx, leaderID, slotID, cmd)
	})
	return proposeErr
}

func observerElapsed(start time.Time) time.Duration {
	elapsed := time.Since(start)
	if elapsed <= 0 {
		return time.Nanosecond
	}
	return elapsed
}

func (c *Cluster) NodeID() multiraft.NodeID {
	if c == nil {
		return 0
	}
	return c.cfg.NodeID
}

// SlotForKey maps a key to a raft slot via CRC32 hashing.
func (c *Cluster) SlotForKey(key string) multiraft.SlotID {
	return c.router.SlotForKey(key)
}

// LeaderOf returns the current leader of the specified slot.
func (c *Cluster) LeaderOf(slotID multiraft.SlotID) (multiraft.NodeID, error) {
	if c == nil || c.router == nil {
		return 0, ErrNotStarted
	}
	return c.router.LeaderOf(slotID)
}

// IsLocal reports whether the given node is the local node.
func (c *Cluster) IsLocal(nodeID multiraft.NodeID) bool {
	if c == nil || c.router == nil {
		return false
	}
	return c.router.IsLocal(nodeID)
}

// Server returns the underlying transport.Server, allowing business layer
// to register additional handlers on the shared listener.
func (c *Cluster) Server() *transport.Server {
	return c.server
}

// RPCMux exposes the shared node RPC service multiplexer used for registering
// additional RPC services on the cluster listener without replacing the
// existing forwarding handler.
func (c *Cluster) RPCMux() *transport.RPCMux {
	return c.rpcMux
}

// Discovery returns the cluster's Discovery instance for creating business pools.
func (c *Cluster) Discovery() Discovery {
	return c.discovery
}

// RPCService issues an RPC request to the given node using the shared cluster transport.
func (c *Cluster) RPCService(ctx context.Context, nodeID multiraft.NodeID, slotID multiraft.SlotID, serviceID uint8, payload []byte) ([]byte, error) {
	if c.stopped.Load() {
		return nil, transport.ErrStopped
	}
	if c.fwdClient == nil {
		return nil, ErrNotStarted
	}
	return c.fwdClient.RPCService(ctx, uint64(nodeID), uint64(slotID), serviceID, payload)
}

// SlotIDs returns the configured control-plane slot ids.
func (c *Cluster) SlotIDs() []multiraft.SlotID {
	slotIDs := make([]multiraft.SlotID, 0, c.cfg.SlotCount)
	for slotID := uint32(1); slotID <= c.cfg.SlotCount; slotID++ {
		slotIDs = append(slotIDs, multiraft.SlotID(slotID))
	}
	return slotIDs
}

func (c *Cluster) WaitForManagedSlotsReady(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	ticker := time.NewTicker(c.managedSlotsReadyPollInterval())
	defer ticker.Stop()

	var lastErr error
	for {
		ready, err := c.managedSlotsReady(ctx)
		if ready {
			return nil
		}
		if err != nil {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			if lastErr != nil {
				return lastErr
			}
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// PeersForSlot returns the configured peers for a control-plane slot.
func (c *Cluster) PeersForSlot(slotID multiraft.SlotID) []multiraft.NodeID {
	if peers, ok := c.assignments.PeersForSlot(slotID); ok {
		return peers
	}
	peers, _ := c.legacyPeersForSlot(slotID)
	return peers
}

func (c *Cluster) ListObservedRuntimeViews(ctx context.Context) ([]controllermeta.SlotRuntimeView, error) {
	if c.controllerClient != nil {
		var views []controllermeta.SlotRuntimeView
		err := c.retryControllerCommand(ctx, func(attemptCtx context.Context) error {
			var err error
			views, err = c.controllerClient.ListRuntimeViews(attemptCtx)
			return err
		})
		if err == nil {
			return views, nil
		}
		if !controllerReadFallbackAllowed(err) || c.controllerMeta == nil {
			return nil, err
		}
	}
	if c.controllerMeta != nil {
		return c.controllerMeta.ListRuntimeViews(ctx)
	}
	return nil, ErrNotStarted
}

func (c *Cluster) ListSlotAssignments(ctx context.Context) ([]controllermeta.SlotAssignment, error) {
	if c.controllerClient != nil {
		var assignments []controllermeta.SlotAssignment
		err := c.retryControllerCommand(ctx, func(attemptCtx context.Context) error {
			var err error
			assignments, err = c.controllerClient.RefreshAssignments(attemptCtx)
			return err
		})
		if err == nil {
			return assignments, nil
		}
		if !controllerReadFallbackAllowed(err) || c.controllerMeta == nil {
			return nil, err
		}
	}
	if c.controllerMeta != nil {
		return c.controllerMeta.ListAssignments(ctx)
	}
	return c.ListCachedAssignments(), nil
}

func controllerReadFallbackAllowed(err error) bool {
	return controllerCommandRetryAllowed(err)
}

func controllerCommandRetryAllowed(err error) bool {
	return errors.Is(err, ErrNotLeader) ||
		errors.Is(err, ErrNoLeader) ||
		errors.Is(err, context.DeadlineExceeded)
}

func (c *Cluster) ListCachedAssignments() []controllermeta.SlotAssignment {
	if c.assignments == nil {
		return nil
	}
	return c.assignments.Snapshot()
}

func (c *Cluster) observationPeersForSlot(slotID multiraft.SlotID) []multiraft.NodeID {
	if peers, ok := c.getRuntimePeers(slotID); ok {
		return peers
	}
	peers, _ := c.legacyPeersForSlot(slotID)
	return peers
}

func (c *Cluster) legacyPeersForSlot(slotID multiraft.SlotID) ([]multiraft.NodeID, bool) {
	for _, slot := range c.cfg.Slots {
		if slot.SlotID != slotID {
			continue
		}
		return append([]multiraft.NodeID(nil), slot.Peers...), true
	}
	return nil, false
}

func (c *Cluster) managedSlotsReady(ctx context.Context) (bool, error) {
	slotIDs := c.SlotIDs()
	if len(slotIDs) == 0 {
		return true, nil
	}
	if c.controllerClient == nil {
		for _, slotID := range slotIDs {
			if _, err := c.LeaderOf(slotID); err != nil {
				return false, err
			}
		}
		return true, nil
	}

	assignments, err := c.ListSlotAssignments(ctx)
	if err != nil {
		return false, err
	}
	if len(assignments) != len(slotIDs) {
		return false, nil
	}

	assignmentByGroup := make(map[uint32]controllermeta.SlotAssignment, len(assignments))
	for _, assignment := range assignments {
		if len(assignment.DesiredPeers) == 0 {
			return false, nil
		}
		assignmentByGroup[assignment.SlotID] = assignment
	}
	for _, slotID := range slotIDs {
		if _, ok := assignmentByGroup[uint32(slotID)]; !ok {
			return false, nil
		}
	}

	for _, slotID := range c.localAssignedSlotIDs(assignments) {
		if _, err := c.LeaderOf(slotID); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (c *Cluster) localAssignedSlotIDs(assignments []controllermeta.SlotAssignment) []multiraft.SlotID {
	if c == nil {
		return nil
	}

	localNodeID := uint64(c.cfg.NodeID)
	slotIDs := make([]multiraft.SlotID, 0, len(assignments))
	for _, assignment := range assignments {
		if assignmentContainsPeer(assignment.DesiredPeers, localNodeID) {
			slotIDs = append(slotIDs, multiraft.SlotID(assignment.SlotID))
		}
	}
	return slotIDs
}

func (c *Cluster) controllerReportAddr() string {
	if c.server != nil && c.server.Listener() != nil {
		return c.server.Listener().Addr().String()
	}
	return c.cfg.ListenAddr
}

func (c *Cluster) setRuntimePeers(slotID multiraft.SlotID, peers []multiraft.NodeID) {
	if c == nil {
		return
	}
	if c.runState == nil {
		c.runState = newRuntimeState()
	}
	c.runState.Set(slotID, peers)
}

func (c *Cluster) getRuntimePeers(slotID multiraft.SlotID) ([]multiraft.NodeID, bool) {
	if c == nil {
		return nil, false
	}
	return c.runState.Get(slotID)
}

func (c *Cluster) deleteRuntimePeers(slotID multiraft.SlotID) {
	if c == nil {
		return
	}
	if c.runState == nil {
		return
	}
	c.runState.Delete(slotID)
}

func nodeIDsFromUint64s(ids []uint64) []multiraft.NodeID {
	peers := make([]multiraft.NodeID, 0, len(ids))
	for _, id := range ids {
		peers = append(peers, multiraft.NodeID(id))
	}
	return peers
}

func (c *Cluster) handleControllerRPC(ctx context.Context, body []byte) ([]byte, error) {
	return (&controllerHandler{cluster: c}).Handle(ctx, body)
}
