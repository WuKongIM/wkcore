package cluster

import (
	"context"
	"encoding/json"
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
	raft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type Cluster struct {
	cfg                         Config
	server                      *transport.Server
	rpcMux                      *transport.RPCMux
	raftPool                    *transport.Pool
	rpcPool                     *transport.Pool
	raftClient                  *transport.Client
	fwdClient                   *transport.Client
	runtime                     *multiraft.Runtime
	router                      *Router
	discovery                   *StaticDiscovery
	controllerMeta              *controllermeta.Store
	controllerRaftDB            *raftstorage.DB
	controllerSM                *slotcontroller.StateMachine
	controller                  *controllerraft.Service
	controllerClient            controllerAPI
	controllerLeaderWaitTimeout time.Duration
	agent                       *slotAgent
	assignments                 *assignmentCache
	runtimePeersMu              sync.RWMutex
	runtimePeers                map[multiraft.SlotID][]multiraft.NodeID
	observationStop             chan struct{}
	observationDone             chan struct{}
	observationCancel           context.CancelFunc
	stopped                     atomic.Bool
}

func NewCluster(cfg Config) (*Cluster, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Cluster{
		cfg:          cfg,
		rpcMux:       transport.NewRPCMux(),
		router:       NewRouter(cfg.SlotCount, cfg.NodeID, nil),
		assignments:  newAssignmentCache(),
		runtimePeers: make(map[multiraft.SlotID][]multiraft.NodeID),
	}, nil
}

func (c *Cluster) Start() error {
	c.discovery = NewStaticDiscovery(c.cfg.Nodes)
	c.observationStop = make(chan struct{})

	if err := c.startServer(); err != nil {
		return err
	}
	c.startPools()
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

	meta, err := controllermeta.Open(c.cfg.ControllerMetaPath)
	if err != nil {
		return fmt.Errorf("open controller meta: %w", err)
	}
	logDB, err := raftstorage.Open(c.cfg.ControllerRaftPath)
	if err != nil {
		_ = meta.Close()
		return fmt.Errorf("open controller raft: %w", err)
	}

	peers := c.cfg.DerivedControllerNodes()
	controllerPeers := make([]controllerraft.Peer, 0, len(peers))
	for _, peer := range peers {
		controllerPeers = append(controllerPeers, controllerraft.Peer{
			NodeID: uint64(peer.NodeID),
			Addr:   peer.Addr,
		})
	}

	sm := slotcontroller.NewStateMachine(meta, slotcontroller.StateMachineConfig{})
	service := controllerraft.NewService(controllerraft.Config{
		NodeID:         uint64(c.cfg.NodeID),
		Peers:          controllerPeers,
		AllowBootstrap: true,
		LogDB:          logDB,
		StateMachine:   sm,
		Server:         c.server,
		RPCMux:         c.rpcMux,
		Pool:           c.raftPool,
	})
	if err := service.Start(context.Background()); err != nil {
		_ = logDB.Close()
		_ = meta.Close()
		return fmt.Errorf("start controller raft: %w", err)
	}

	c.controllerMeta = meta
	c.controllerRaftDB = logDB
	c.controllerSM = sm
	c.controller = service
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

	c.observationDone = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	c.observationCancel = cancel
	go func() {
		defer close(c.observationDone)
		ticker := time.NewTicker(controllerObservationInterval)
		defer ticker.Stop()

		for {
			c.observeOnce(ctx)
			c.controllerTickOnce(ctx)
			select {
			case <-c.observationStop:
				return
			case <-ticker.C:
			}
		}
	}()
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
			return err
		}
		return nil
	}
	c.setRuntimePeers(g.SlotID, g.Peers)
	if err := c.runtime.BootstrapSlot(ctx, multiraft.BootstrapSlotRequest{
		Slot:   opts,
		Voters: g.Peers,
	}); err != nil {
		c.deleteRuntimePeers(g.SlotID)
		return err
	}
	return nil
}

func (c *Cluster) Stop() {
	c.stopped.Store(true)
	if c.observationStop != nil {
		if c.observationCancel != nil {
			c.observationCancel()
			c.observationCancel = nil
		}
		close(c.observationStop)
		if c.observationDone != nil {
			<-c.observationDone
			c.observationDone = nil
		}
		c.observationStop = nil
	}

	if c.server != nil {
		c.server.Stop()
	}
	if c.controller != nil {
		_ = c.controller.Stop()
	}
	if c.runtime != nil {
		_ = c.runtime.Close()
	}
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
	if c.controllerRaftDB != nil {
		_ = c.controllerRaftDB.Close()
	}
	if c.controllerMeta != nil {
		_ = c.controllerMeta.Close()
	}
}

func (c *Cluster) observeOnce(ctx context.Context) {
	if c.agent == nil || c.runtime == nil || c.stopped.Load() {
		return
	}
	_ = c.agent.HeartbeatOnce(ctx)
	assignCtx, cancel := withControllerTimeout(ctx)
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

	tickCtx, cancel := withControllerTimeout(ctx)
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

	proposeCtx, cancel := withControllerTimeout(ctx)
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
	if c.stopped.Load() {
		return transport.ErrStopped
	}
	if c.router == nil || c.runtime == nil {
		return ErrNotStarted
	}
	const maxRetries = 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 50ms, 100ms
			backoff := time.Duration(attempt) * 50 * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		leaderID, err := c.router.LeaderOf(slotID)
		if err != nil {
			return err
		}
		if c.router.IsLocal(leaderID) {
			future, err := c.runtime.Propose(ctx, slotID, cmd)
			if err != nil {
				return err
			}
			_, err = future.Wait(ctx)
			return err
		}
		err = c.forwardToLeader(ctx, leaderID, slotID, cmd)
		if errors.Is(err, ErrNotLeader) {
			continue
		}
		return err
	}
	return ErrLeaderNotStable
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

	ticker := time.NewTicker(100 * time.Millisecond)
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

	assignmentByGroup := make(map[uint32]struct{}, len(assignments))
	for _, assignment := range assignments {
		if len(assignment.DesiredPeers) == 0 {
			return false, nil
		}
		assignmentByGroup[assignment.SlotID] = struct{}{}
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

	c.runtimePeersMu.Lock()
	c.runtimePeers[slotID] = append([]multiraft.NodeID(nil), peers...)
	c.runtimePeersMu.Unlock()
}

func (c *Cluster) getRuntimePeers(slotID multiraft.SlotID) ([]multiraft.NodeID, bool) {
	if c == nil {
		return nil, false
	}

	c.runtimePeersMu.RLock()
	peers, ok := c.runtimePeers[slotID]
	c.runtimePeersMu.RUnlock()
	if !ok {
		return nil, false
	}
	return append([]multiraft.NodeID(nil), peers...), true
}

func (c *Cluster) deleteRuntimePeers(slotID multiraft.SlotID) {
	if c == nil {
		return
	}

	c.runtimePeersMu.Lock()
	delete(c.runtimePeers, slotID)
	c.runtimePeersMu.Unlock()
}

func nodeIDsFromUint64s(ids []uint64) []multiraft.NodeID {
	peers := make([]multiraft.NodeID, 0, len(ids))
	for _, id := range ids {
		peers = append(peers, multiraft.NodeID(id))
	}
	return peers
}

func (c *Cluster) handleControllerRPC(ctx context.Context, body []byte) ([]byte, error) {
	var req controllerRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	if c.controller == nil || c.controllerMeta == nil {
		return nil, ErrNotStarted
	}

	marshalRedirect := func() ([]byte, error) {
		return json.Marshal(controllerRPCResponse{
			NotLeader: true,
			LeaderID:  c.controller.LeaderID(),
		})
	}

	switch req.Kind {
	case controllerRPCHeartbeat:
		if req.Report == nil {
			return nil, ErrInvalidConfig
		}
		if leaderID := c.controller.LeaderID(); leaderID != uint64(c.cfg.NodeID) {
			return marshalRedirect()
		}
		proposeCtx, cancel := withControllerTimeout(ctx)
		defer cancel()
		if err := c.controller.Propose(proposeCtx, slotcontroller.Command{
			Kind:   slotcontroller.CommandKindNodeHeartbeat,
			Report: req.Report,
		}); err != nil {
			if errors.Is(err, controllerraft.ErrNotLeader) {
				return marshalRedirect()
			}
			return nil, err
		}
		return json.Marshal(controllerRPCResponse{})
	case controllerRPCTaskResult:
		if leaderID := c.controller.LeaderID(); leaderID != uint64(c.cfg.NodeID) {
			return marshalRedirect()
		}
		if req.Advance == nil {
			return nil, ErrInvalidConfig
		}
		advance := &slotcontroller.TaskAdvance{
			SlotID:  req.Advance.SlotID,
			Attempt: req.Advance.Attempt,
			Now:     req.Advance.Now,
		}
		if req.Advance.Err != "" {
			advance.Err = errors.New(req.Advance.Err)
		}
		proposeCtx, cancel := withControllerTimeout(ctx)
		defer cancel()
		if err := c.controller.Propose(proposeCtx, slotcontroller.Command{
			Kind:    slotcontroller.CommandKindTaskResult,
			Advance: advance,
		}); err != nil {
			if errors.Is(err, controllerraft.ErrNotLeader) {
				return marshalRedirect()
			}
			return nil, err
		}
		return json.Marshal(controllerRPCResponse{})
	case controllerRPCListAssignments:
		if leaderID := c.controller.LeaderID(); leaderID != uint64(c.cfg.NodeID) {
			return marshalRedirect()
		}
		assignments, err := c.controllerMeta.ListAssignments(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(controllerRPCResponse{Assignments: assignments})
	case controllerRPCListNodes:
		if leaderID := c.controller.LeaderID(); leaderID != uint64(c.cfg.NodeID) {
			return marshalRedirect()
		}
		nodes, err := c.controllerMeta.ListNodes(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(controllerRPCResponse{Nodes: nodes})
	case controllerRPCListRuntimeViews:
		if leaderID := c.controller.LeaderID(); leaderID != uint64(c.cfg.NodeID) {
			return marshalRedirect()
		}
		views, err := c.controllerMeta.ListRuntimeViews(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(controllerRPCResponse{RuntimeViews: views})
	case controllerRPCOperator:
		if leaderID := c.controller.LeaderID(); leaderID != uint64(c.cfg.NodeID) {
			return marshalRedirect()
		}
		if req.Op == nil {
			return nil, ErrInvalidConfig
		}
		proposeCtx, cancel := withControllerTimeout(ctx)
		defer cancel()
		if err := c.controller.Propose(proposeCtx, slotcontroller.Command{
			Kind: slotcontroller.CommandKindOperatorRequest,
			Op:   req.Op,
		}); err != nil {
			if errors.Is(err, controllerraft.ErrNotLeader) {
				return marshalRedirect()
			}
			return nil, err
		}
		return json.Marshal(controllerRPCResponse{})
	case controllerRPCGetTask:
		if leaderID := c.controller.LeaderID(); leaderID != uint64(c.cfg.NodeID) {
			return marshalRedirect()
		}
		task, err := c.controllerMeta.GetTask(ctx, req.SlotID)
		if errors.Is(err, controllermeta.ErrNotFound) {
			return json.Marshal(controllerRPCResponse{NotFound: true})
		}
		if err != nil {
			return nil, err
		}
		return json.Marshal(controllerRPCResponse{Task: &task})
	case controllerRPCForceReconcile:
		if leaderID := c.controller.LeaderID(); leaderID != uint64(c.cfg.NodeID) {
			return marshalRedirect()
		}
		if err := c.forceReconcileOnLeader(ctx, req.SlotID); err != nil {
			return nil, err
		}
		return json.Marshal(controllerRPCResponse{})
	default:
		return nil, ErrInvalidConfig
	}
}
