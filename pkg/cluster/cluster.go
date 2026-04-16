package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

type agentResources struct {
	agent       *slotAgent
	assignments *assignmentCache
}

type hashSlotRuntimeResources struct {
	runtimeStateMachinesMu sync.RWMutex
	runtimeStateMachines   map[multiraft.SlotID]hashSlotOwnershipUpdater
}

type Cluster struct {
	cfg    Config
	logger wklog.Logger
	obs    ObserverHooks
	transportResources
	runtime *multiraft.Runtime
	router  *Router
	controllerResources
	agentResources
	migrationWorker       hashSlotMigrationWorker
	pendingHashSlotAborts map[uint16]pendingHashSlotAbort
	runState              *runtimeState
	hashSlotRuntimeResources
	observer *observerLoop
	managedSlotResources
	stopped atomic.Bool
}

type pendingHashSlotAbort struct {
	migration             HashSlotMigration
	lastAbortTableVersion uint64
}

type hashSlotOwnershipUpdater interface {
	UpdateOwnedHashSlots([]uint16)
}

type hashSlotMigrationRuntimeUpdater interface {
	UpdateOutgoingDeltaTargets(map[uint16]multiraft.SlotID)
	UpdateIncomingDeltaHashSlots([]uint16)
}

type hashSlotDeltaForwarderInstaller interface {
	SetDeltaForwarder(func(context.Context, multiraft.SlotID, multiraft.Command) error)
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
		router:   NewRouter(NewHashSlotTable(cfg.effectiveHashSlotCount(), int(cfg.effectiveInitialSlotCount())), cfg.NodeID, nil),
		runState: newRuntimeState(),
		agentResources: agentResources{
			assignments: newAssignmentCache(),
		},
		hashSlotRuntimeResources: hashSlotRuntimeResources{
			runtimeStateMachines: make(map[multiraft.SlotID]hashSlotOwnershipUpdater),
		},
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

func (c *Cluster) transportLogger() wklog.Logger {
	if c == nil || c.logger == nil {
		return wklog.NewNop()
	}
	return c.logger.Named("transport")
}

func (c *Cluster) controllerLogger() wklog.Logger {
	if c == nil || c.logger == nil {
		return wklog.NewNop()
	}
	return c.logger.Named("controller")
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
	c.server = transport.NewServerWithConfig(transport.ServerConfig{
		ConnConfig: transport.ConnConfig{
			Observer: c.cfg.TransportObserver,
		},
	})
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
		Observer:    c.cfg.TransportObserver,
	})
	c.rpcPool = transport.NewPool(transport.PoolConfig{
		Discovery:   c.discovery,
		Size:        c.cfg.PoolSize,
		DialTimeout: c.cfg.DialTimeout,
		QueueSizes:  [3]int{0, 1024, 256},
		DefaultPri:  transport.PriorityRPC,
		Observer:    c.cfg.TransportObserver,
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
	table, err := c.ensureControllerHashSlotTable(context.Background(), host.meta)
	if err != nil {
		host.Stop()
		return fmt.Errorf("ensure controller hash slot table: %w", err)
	}
	host.storeHashSlotTableSnapshot(table)
	if c.router != nil {
		c.router.UpdateHashSlotTable(table)
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
		Transport:    &raftTransport{client: c.raftClient, logger: c.transportLogger()},
		Raft: multiraft.RaftOptions{
			ElectionTick:  c.cfg.ElectionTick,
			HeartbeatTick: c.cfg.HeartbeatTick,
		},
	})
	if err != nil {
		return fmt.Errorf("create runtime: %w", err)
	}

	if c.router == nil {
		c.router = NewRouter(NewHashSlotTable(c.cfg.effectiveHashSlotCount(), int(c.cfg.effectiveInitialSlotCount())), c.cfg.NodeID, c.runtime)
	} else {
		c.router.runtime = c.runtime
	}
	return nil
}

func (c *Cluster) startControllerClient() {
	if !c.cfg.ControllerEnabled() {
		return
	}
	if c.migrationWorker == nil {
		c.migrationWorker = newHashSlotMigrationWorker()
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
	sm, err := c.newStateMachine(g.SlotID)
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
	_ = c.observeHashSlotMigrations(ctx)
}

func (c *Cluster) controllerTickOnce(ctx context.Context) {
	if c.controller == nil || c.controllerMeta == nil || c.stopped.Load() {
		return
	}
	if c.controller.LeaderID() != uint64(c.cfg.NodeID) {
		return
	}
	if c.controllerHost != nil && !c.controllerHost.warmupComplete() {
		return
	}

	start := time.Now()
	state, err := c.snapshotPlannerState(ctx)
	if err != nil {
		return
	}
	planner := slotcontroller.NewPlanner(slotcontroller.PlannerConfig{
		SlotCount: c.cfg.effectiveInitialSlotCount(),
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
	err = c.controller.Propose(proposeCtx, slotcontroller.Command{
		Kind:       slotcontroller.CommandKindAssignmentTaskUpdate,
		Assignment: &decision.Assignment,
		Task:       decision.Task,
	})
	cancel()
	if err != nil {
		return
	}
	if hook := c.obs.OnControllerDecision; hook != nil {
		hook(decision.SlotID, controllerTaskKindName(decision.Task.Kind), observerElapsed(start))
	}
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
	tasks, err := c.controllerMeta.ListTasks(ctx)
	if err != nil {
		return slotcontroller.PlannerState{}, err
	}
	views, err := c.plannerRuntimeViews(ctx)
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

func (c *Cluster) plannerRuntimeViews(ctx context.Context) ([]controllermeta.SlotRuntimeView, error) {
	if c == nil {
		return nil, ErrNotStarted
	}
	if c.controllerHost != nil && c.controllerHost.IsLeader(c.cfg.NodeID) {
		if snapshot, ok := c.controllerHost.plannerSnapshot(); ok {
			return snapshot.RuntimeViews, nil
		}
		return nil, nil
	}
	return c.controllerMeta.ListRuntimeViews(ctx)
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
// It exists for legacy one-hash-slot-per-slot callers; hash-slot-aware callers should use
// ProposeWithHashSlot so writes remain valid after a physical slot owns multiple hash slots.
func (c *Cluster) Propose(ctx context.Context, slotID multiraft.SlotID, cmd []byte) error {
	hashSlot, err := c.legacyProposeHashSlot(slotID)
	if err != nil {
		return err
	}
	return c.ProposeWithHashSlot(ctx, slotID, hashSlot, cmd)
}

// ProposeWithHashSlot submits a command envelope carrying the logical hash slot.
func (c *Cluster) ProposeWithHashSlot(ctx context.Context, slotID multiraft.SlotID, hashSlot uint16, cmd []byte) error {
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
		payload := encodeProposalPayload(hashSlot, cmd)
		leaderID, err := c.router.LeaderOf(slotID)
		if err != nil {
			return err
		}
		if c.router.IsLocal(leaderID) {
			future, err := c.runtime.Propose(attemptCtx, slotID, payload)
			if err != nil {
				return err
			}
			_, err = future.Wait(attemptCtx)
			return err
		}
		return c.forwardToLeader(attemptCtx, leaderID, slotID, payload)
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

func controllerTaskKindName(kind controllermeta.TaskKind) string {
	switch kind {
	case controllermeta.TaskKindBootstrap:
		return "bootstrap"
	case controllermeta.TaskKindRepair:
		return "repair"
	case controllermeta.TaskKindRebalance:
		return "rebalance"
	default:
		return "unknown"
	}
}

func controllerTaskResult(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "fail"
	}
}

func (c *Cluster) NodeID() multiraft.NodeID {
	if c == nil {
		return 0
	}
	return c.cfg.NodeID
}

func (c *Cluster) legacyProposeHashSlot(slotID multiraft.SlotID) (uint16, error) {
	if c == nil || c.router == nil {
		return 0, ErrNotStarted
	}
	hashSlots := c.router.HashSlotsOf(slotID)
	switch len(hashSlots) {
	case 0:
		return 0, ErrSlotNotFound
	case 1:
		return hashSlots[0], nil
	default:
		return 0, fmt.Errorf("%w for slot %d; use ProposeWithHashSlot", ErrHashSlotRequired, slotID)
	}
}

// SlotForKey maps a key to a raft slot via CRC32 hashing.
func (c *Cluster) SlotForKey(key string) multiraft.SlotID {
	return c.router.SlotForKey(key)
}

// HashSlotForKey maps a key to its logical hash slot.
func (c *Cluster) HashSlotForKey(key string) uint16 {
	if c == nil || c.router == nil {
		return 0
	}
	return c.router.HashSlotForKey(key)
}

func (c *Cluster) HashSlotTableVersion() uint64 {
	if c == nil || c.router == nil {
		return 0
	}
	table := c.router.hashSlotTable.Load()
	if table == nil {
		return 0
	}
	return table.Version()
}

func (c *Cluster) defaultHashSlotTable() *HashSlotTable {
	if c == nil {
		return nil
	}
	return NewHashSlotTable(c.cfg.effectiveHashSlotCount(), int(c.cfg.effectiveInitialSlotCount()))
}

func (c *Cluster) applyHashSlotTablePayload(data []byte) error {
	if c == nil || c.router == nil || len(data) == 0 {
		return nil
	}
	table, err := DecodeHashSlotTable(data)
	if err != nil {
		return err
	}
	c.updateRuntimeHashSlotTable(table)
	return nil
}

func (c *Cluster) ensureControllerHashSlotTable(ctx context.Context, store *controllermeta.Store) (*HashSlotTable, error) {
	if store == nil {
		return nil, ErrNotStarted
	}
	table, err := store.LoadHashSlotTable(ctx)
	if err == nil {
		return table, nil
	}
	if !errors.Is(err, controllermeta.ErrNotFound) {
		return nil, err
	}
	table = c.defaultHashSlotTable()
	if table == nil {
		return nil, ErrNotStarted
	}
	if err := store.SaveHashSlotTable(ctx, table); err != nil {
		return nil, err
	}
	return table, nil
}

func (c *Cluster) syncRouterHashSlotTableFromStore(ctx context.Context) error {
	if c == nil || c.controllerMeta == nil {
		return nil
	}
	table, err := c.controllerMeta.LoadHashSlotTable(ctx)
	if errors.Is(err, controllermeta.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	c.updateRuntimeHashSlotTable(table)
	return nil
}

func (c *Cluster) updateRuntimeHashSlotTable(table *HashSlotTable) {
	if c == nil {
		return
	}
	if c.router != nil {
		c.router.UpdateHashSlotTable(table)
	}

	c.runtimeStateMachinesMu.RLock()
	registrations := make(map[multiraft.SlotID]hashSlotOwnershipUpdater, len(c.runtimeStateMachines))
	for slotID, updater := range c.runtimeStateMachines {
		registrations[slotID] = updater
	}
	c.runtimeStateMachinesMu.RUnlock()

	for slotID, updater := range registrations {
		if updater == nil {
			continue
		}
		var hashSlots []uint16
		var outgoingDeltaTargets map[uint16]multiraft.SlotID
		var incomingDeltaHashSlots []uint16
		if table != nil {
			hashSlots = table.HashSlotsOf(slotID)
			outgoingDeltaTargets, incomingDeltaHashSlots = deltaMigrationRuntimeForSlot(table, slotID)
		}
		updater.UpdateOwnedHashSlots(hashSlots)
		if migrationUpdater, ok := updater.(hashSlotMigrationRuntimeUpdater); ok {
			migrationUpdater.UpdateOutgoingDeltaTargets(outgoingDeltaTargets)
			migrationUpdater.UpdateIncomingDeltaHashSlots(incomingDeltaHashSlots)
		}
	}
}

func (c *Cluster) registerRuntimeStateMachine(slotID multiraft.SlotID, sm multiraft.StateMachine) {
	if c == nil || sm == nil {
		return
	}
	if installer, ok := sm.(hashSlotDeltaForwarderInstaller); ok {
		installer.SetDeltaForwarder(c.makeHashSlotDeltaForwarder())
	}
	updater, ok := sm.(hashSlotOwnershipUpdater)
	if !ok {
		return
	}
	c.runtimeStateMachinesMu.Lock()
	if c.runtimeStateMachines == nil {
		c.runtimeStateMachines = make(map[multiraft.SlotID]hashSlotOwnershipUpdater)
	}
	c.runtimeStateMachines[slotID] = updater
	c.runtimeStateMachinesMu.Unlock()
}

func (c *Cluster) unregisterRuntimeStateMachine(slotID multiraft.SlotID) {
	if c == nil {
		return
	}
	c.runtimeStateMachinesMu.Lock()
	delete(c.runtimeStateMachines, slotID)
	c.runtimeStateMachinesMu.Unlock()
}

func (c *Cluster) runtimeStateMachine(slotID multiraft.SlotID) (hashSlotOwnershipUpdater, bool) {
	if c == nil {
		return nil, false
	}
	c.runtimeStateMachinesMu.RLock()
	defer c.runtimeStateMachinesMu.RUnlock()
	sm, ok := c.runtimeStateMachines[slotID]
	return sm, ok
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
	initialSlotCount := c.cfg.effectiveInitialSlotCount()
	slotIDs := make([]multiraft.SlotID, 0, initialSlotCount)
	for slotID := uint32(1); slotID <= initialSlotCount; slotID++ {
		slotIDs = append(slotIDs, multiraft.SlotID(slotID))
	}
	return slotIDs
}

func (c *Cluster) newStateMachine(slotID multiraft.SlotID) (multiraft.StateMachine, error) {
	if c.cfg.NewStateMachineWithHashSlots != nil {
		hashSlots := []uint16{uint16(slotID)}
		if c.router != nil {
			if assigned := c.router.HashSlotsOf(slotID); len(assigned) > 0 {
				hashSlots = assigned
			}
		}
		sm, err := c.cfg.NewStateMachineWithHashSlots(slotID, hashSlots)
		if err != nil {
			return nil, err
		}
		c.registerRuntimeStateMachine(slotID, sm)
		return sm, nil
	}
	sm, err := c.cfg.NewStateMachine(slotID)
	if err != nil {
		return nil, err
	}
	c.registerRuntimeStateMachine(slotID, sm)
	return sm, nil
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

func (c *Cluster) ListNodes(ctx context.Context) ([]controllermeta.ClusterNode, error) {
	if c.controllerClient != nil {
		var nodes []controllermeta.ClusterNode
		err := c.retryControllerCommand(ctx, func(attemptCtx context.Context) error {
			var err error
			nodes, err = c.controllerClient.ListNodes(attemptCtx)
			return err
		})
		if err == nil {
			return nodes, nil
		}
		if !controllerReadFallbackAllowed(err) || c.controllerMeta == nil {
			return nil, err
		}
	}
	if c.controllerMeta != nil {
		return c.controllerMeta.ListNodes(ctx)
	}
	return nil, ErrNotStarted
}

func (c *Cluster) ListObservedRuntimeViews(ctx context.Context) ([]controllermeta.SlotRuntimeView, error) {
	if views, ok := c.localObservedRuntimeViews(); ok {
		return views, nil
	}
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

func (c *Cluster) localObservedRuntimeViews() ([]controllermeta.SlotRuntimeView, bool) {
	if c == nil || c.controllerHost == nil || !c.controllerHost.IsLeader(c.cfg.NodeID) {
		return nil, false
	}
	return c.controllerHost.snapshotObservations().RuntimeViews, true
}

func (c *Cluster) ListTasks(ctx context.Context) ([]controllermeta.ReconcileTask, error) {
	if c.controllerClient != nil {
		var tasks []controllermeta.ReconcileTask
		err := c.retryControllerCommand(ctx, func(attemptCtx context.Context) error {
			var err error
			tasks, err = c.controllerClient.ListTasks(attemptCtx)
			return err
		})
		if err == nil {
			return tasks, nil
		}
		if !controllerReadFallbackAllowed(err) || c.controllerMeta == nil {
			return nil, err
		}
	}
	if c.controllerMeta != nil {
		return c.controllerMeta.ListTasks(ctx)
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
		assignments, err := c.controllerMeta.ListAssignments(ctx)
		if err != nil {
			return nil, err
		}
		if err := c.syncRouterHashSlotTableFromStore(ctx); err != nil {
			return nil, err
		}
		return assignments, nil
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
		c.unregisterRuntimeStateMachine(slotID)
		return
	}
	c.runState.Delete(slotID)
	c.unregisterRuntimeStateMachine(slotID)
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
