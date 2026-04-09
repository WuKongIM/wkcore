package raftcluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/controllerraft"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/groupcontroller"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
	"github.com/WuKongIM/WuKongIM/pkg/storage/raftstorage"
	"github.com/WuKongIM/WuKongIM/pkg/transport/nodetransport"
	raft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type Cluster struct {
	cfg               Config
	server            *nodetransport.Server
	rpcMux            *nodetransport.RPCMux
	raftPool          *nodetransport.Pool
	raftClient        *nodetransport.Client
	fwdClient         *nodetransport.Client
	runtime           *multiraft.Runtime
	router            *Router
	discovery         *StaticDiscovery
	controllerMeta    *controllermeta.Store
	controllerRaftDB  *raftstorage.DB
	controllerSM      *groupcontroller.StateMachine
	controller        *controllerraft.Service
	controllerClient  controllerAPI
	agent             *groupAgent
	assignments       *assignmentCache
	runtimePeersMu    sync.RWMutex
	runtimePeers      map[multiraft.GroupID][]multiraft.NodeID
	observationStop   chan struct{}
	observationDone   chan struct{}
	observationCancel context.CancelFunc
	stopped           atomic.Bool
}

func NewCluster(cfg Config) (*Cluster, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Cluster{
		cfg:          cfg,
		rpcMux:       nodetransport.NewRPCMux(),
		router:       NewRouter(cfg.GroupCount, cfg.NodeID, nil),
		assignments:  newAssignmentCache(),
		runtimePeers: make(map[multiraft.GroupID][]multiraft.NodeID),
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
	if err := c.seedLegacyGroupsIfConfigured(); err != nil {
		c.Stop()
		return err
	}
	return nil
}

func (c *Cluster) startServer() error {
	c.server = nodetransport.NewServer()
	c.server.Handle(msgTypeRaft, c.handleRaftMessage)
	c.rpcMux.Handle(rpcServiceForward, c.handleForwardRPC)
	c.rpcMux.Handle(rpcServiceController, c.handleControllerRPC)
	c.rpcMux.Handle(rpcServiceManagedGroup, c.handleManagedGroupRPC)
	c.server.HandleRPCMux(c.rpcMux)
	if err := c.server.Start(c.cfg.ListenAddr); err != nil {
		return fmt.Errorf("start server: %w", err)
	}
	return nil
}

func (c *Cluster) startPools() {
	c.raftPool = nodetransport.NewPool(c.discovery, c.cfg.PoolSize, c.cfg.DialTimeout)
	c.raftClient = nodetransport.NewClient(c.raftPool)
	c.fwdClient = nodetransport.NewClient(c.raftPool)
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

	sm := groupcontroller.NewStateMachine(meta, groupcontroller.StateMachineConfig{})
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
		c.router = NewRouter(c.cfg.GroupCount, c.cfg.NodeID, c.runtime)
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
	c.agent = &groupAgent{
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

func (c *Cluster) seedLegacyGroupsIfConfigured() error {
	if c.cfg.ControllerEnabled() {
		return nil
	}
	ctx := context.Background()
	for _, g := range c.cfg.Groups {
		if err := c.openOrBootstrapGroup(ctx, g); err != nil {
			return fmt.Errorf("open group %d: %w", g.GroupID, err)
		}
	}
	return nil
}

func (c *Cluster) openOrBootstrapGroup(ctx context.Context, g GroupConfig) error {
	storage, err := c.cfg.NewStorage(g.GroupID)
	if err != nil {
		return fmt.Errorf("create storage for group %d: %w", g.GroupID, err)
	}
	sm, err := c.cfg.NewStateMachine(g.GroupID)
	if err != nil {
		return fmt.Errorf("create state machine for group %d: %w", g.GroupID, err)
	}
	opts := multiraft.GroupOptions{
		ID:           g.GroupID,
		Storage:      storage,
		StateMachine: sm,
	}

	initialState, err := storage.InitialState(ctx)
	if err != nil {
		return err
	}
	if !raft.IsEmptyHardState(initialState.HardState) {
		c.setRuntimePeers(g.GroupID, nodeIDsFromUint64s(initialState.ConfState.Voters))
		if err := c.runtime.OpenGroup(ctx, opts); err != nil {
			c.deleteRuntimePeers(g.GroupID)
			return err
		}
		return nil
	}
	c.setRuntimePeers(g.GroupID, g.Peers)
	if err := c.runtime.BootstrapGroup(ctx, multiraft.BootstrapGroupRequest{
		Group:  opts,
		Voters: g.Peers,
	}); err != nil {
		c.deleteRuntimePeers(g.GroupID)
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
	_ = c.controller.Propose(tickCtx, groupcontroller.Command{
		Kind:    groupcontroller.CommandKindEvaluateTimeouts,
		Advance: &groupcontroller.TaskAdvance{Now: time.Now()},
	})
	cancel()

	state, err := c.snapshotPlannerState(ctx)
	if err != nil {
		return
	}
	planner := groupcontroller.NewPlanner(groupcontroller.PlannerConfig{
		GroupCount: c.cfg.GroupCount,
		ReplicaN:   c.cfg.GroupReplicaN,
	})
	decision, err := planner.NextDecision(ctx, state)
	if err != nil || decision.GroupID == 0 || decision.Task == nil {
		return
	}
	if _, exists := state.Tasks[decision.GroupID]; exists {
		return
	}

	proposeCtx, cancel := withControllerTimeout(ctx)
	_ = c.controller.Propose(proposeCtx, groupcontroller.Command{
		Kind:       groupcontroller.CommandKindAssignmentTaskUpdate,
		Assignment: &decision.Assignment,
		Task:       decision.Task,
	})
	cancel()
}

func (c *Cluster) snapshotPlannerState(ctx context.Context) (groupcontroller.PlannerState, error) {
	nodes, err := c.controllerMeta.ListNodes(ctx)
	if err != nil {
		return groupcontroller.PlannerState{}, err
	}
	assignments, err := c.controllerMeta.ListAssignments(ctx)
	if err != nil {
		return groupcontroller.PlannerState{}, err
	}
	views, err := c.controllerMeta.ListRuntimeViews(ctx)
	if err != nil {
		return groupcontroller.PlannerState{}, err
	}
	tasks, err := c.controllerMeta.ListTasks(ctx)
	if err != nil {
		return groupcontroller.PlannerState{}, err
	}
	state := groupcontroller.PlannerState{
		Now:         time.Now(),
		Nodes:       make(map[uint64]controllermeta.ClusterNode, len(nodes)),
		Assignments: make(map[uint32]controllermeta.GroupAssignment, len(assignments)),
		Runtime:     make(map[uint32]controllermeta.GroupRuntimeView, len(views)),
		Tasks:       make(map[uint32]controllermeta.ReconcileTask, len(tasks)),
	}
	for _, node := range nodes {
		state.Nodes[node.NodeID] = node
	}
	for _, assignment := range assignments {
		state.Assignments[assignment.GroupID] = assignment
	}
	for _, view := range views {
		state.Runtime[view.GroupID] = view
	}
	for _, task := range tasks {
		state.Tasks[task.GroupID] = task
	}
	return state, nil
}

// handleRaftMessage is the server handler for msgTypeRaft.
func (c *Cluster) handleRaftMessage(_ net.Conn, body []byte) {
	if c.runtime == nil {
		return
	}
	groupID, data, err := decodeRaftBody(body)
	if err != nil {
		return
	}
	var msg raftpb.Message
	if err := msg.Unmarshal(data); err != nil {
		return
	}
	_ = c.runtime.Step(context.Background(), multiraft.Envelope{
		GroupID: multiraft.GroupID(groupID),
		Message: msg,
	})
}

// Propose submits a command to the specified group, automatically handling leader forwarding.
func (c *Cluster) Propose(ctx context.Context, groupID multiraft.GroupID, cmd []byte) error {
	if c.stopped.Load() {
		return nodetransport.ErrStopped
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
		leaderID, err := c.router.LeaderOf(groupID)
		if err != nil {
			return err
		}
		if c.router.IsLocal(leaderID) {
			future, err := c.runtime.Propose(ctx, groupID, cmd)
			if err != nil {
				return err
			}
			_, err = future.Wait(ctx)
			return err
		}
		err = c.forwardToLeader(ctx, leaderID, groupID, cmd)
		if errors.Is(err, ErrNotLeader) {
			continue
		}
		return err
	}
	return ErrLeaderNotStable
}

// SlotForKey maps a key to a raft group via CRC32 hashing.
func (c *Cluster) SlotForKey(key string) multiraft.GroupID {
	return c.router.SlotForKey(key)
}

// LeaderOf returns the current leader of the specified group.
func (c *Cluster) LeaderOf(groupID multiraft.GroupID) (multiraft.NodeID, error) {
	if c == nil || c.router == nil {
		return 0, ErrNotStarted
	}
	return c.router.LeaderOf(groupID)
}

// IsLocal reports whether the given node is the local node.
func (c *Cluster) IsLocal(nodeID multiraft.NodeID) bool {
	if c == nil || c.router == nil {
		return false
	}
	return c.router.IsLocal(nodeID)
}

// Server returns the underlying nodetransport.Server, allowing business layer
// to register additional handlers on the shared listener.
func (c *Cluster) Server() *nodetransport.Server {
	return c.server
}

// RPCMux exposes the shared node RPC service multiplexer used for registering
// additional RPC services on the cluster listener without replacing the
// existing forwarding handler.
func (c *Cluster) RPCMux() *nodetransport.RPCMux {
	return c.rpcMux
}

// Discovery returns the cluster's Discovery instance for creating business pools.
func (c *Cluster) Discovery() Discovery {
	return c.discovery
}

// RPCService issues an RPC request to the given node using the shared cluster transport.
func (c *Cluster) RPCService(ctx context.Context, nodeID multiraft.NodeID, groupID multiraft.GroupID, serviceID uint8, payload []byte) ([]byte, error) {
	if c.stopped.Load() {
		return nil, nodetransport.ErrStopped
	}
	if c.fwdClient == nil {
		return nil, ErrNotStarted
	}
	return c.fwdClient.RPCService(ctx, uint64(nodeID), uint64(groupID), serviceID, payload)
}

// GroupIDs returns the configured control-plane group ids.
func (c *Cluster) GroupIDs() []multiraft.GroupID {
	groupIDs := make([]multiraft.GroupID, 0, c.cfg.GroupCount)
	for groupID := uint32(1); groupID <= c.cfg.GroupCount; groupID++ {
		groupIDs = append(groupIDs, multiraft.GroupID(groupID))
	}
	return groupIDs
}

func (c *Cluster) WaitForManagedGroupsReady(ctx context.Context) error {
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
		ready, err := c.managedGroupsReady(ctx)
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

// PeersForGroup returns the configured peers for a control-plane group.
func (c *Cluster) PeersForGroup(groupID multiraft.GroupID) []multiraft.NodeID {
	if peers, ok := c.assignments.PeersForGroup(groupID); ok {
		return peers
	}
	peers, _ := c.legacyPeersForGroup(groupID)
	return peers
}

func (c *Cluster) ListObservedRuntimeViews(ctx context.Context) ([]controllermeta.GroupRuntimeView, error) {
	if c.controllerClient != nil {
		var views []controllermeta.GroupRuntimeView
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

func (c *Cluster) ListGroupAssignments(ctx context.Context) ([]controllermeta.GroupAssignment, error) {
	if c.controllerClient != nil {
		var assignments []controllermeta.GroupAssignment
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

func (c *Cluster) ListCachedAssignments() []controllermeta.GroupAssignment {
	if c.assignments == nil {
		return nil
	}
	return c.assignments.Snapshot()
}

func (c *Cluster) observationPeersForGroup(groupID multiraft.GroupID) []multiraft.NodeID {
	if peers, ok := c.getRuntimePeers(groupID); ok {
		return peers
	}
	peers, _ := c.legacyPeersForGroup(groupID)
	return peers
}

func (c *Cluster) legacyPeersForGroup(groupID multiraft.GroupID) ([]multiraft.NodeID, bool) {
	for _, group := range c.cfg.Groups {
		if group.GroupID != groupID {
			continue
		}
		return append([]multiraft.NodeID(nil), group.Peers...), true
	}
	return nil, false
}

func (c *Cluster) managedGroupsReady(ctx context.Context) (bool, error) {
	groupIDs := c.GroupIDs()
	if len(groupIDs) == 0 {
		return true, nil
	}
	if c.controllerClient == nil {
		for _, groupID := range groupIDs {
			if _, err := c.LeaderOf(groupID); err != nil {
				return false, err
			}
		}
		return true, nil
	}

	assignments, err := c.ListGroupAssignments(ctx)
	if err != nil {
		return false, err
	}
	if len(assignments) != len(groupIDs) {
		return false, nil
	}

	assignmentByGroup := make(map[uint32]struct{}, len(assignments))
	for _, assignment := range assignments {
		if len(assignment.DesiredPeers) == 0 {
			return false, nil
		}
		assignmentByGroup[assignment.GroupID] = struct{}{}
	}
	for _, groupID := range groupIDs {
		if _, ok := assignmentByGroup[uint32(groupID)]; !ok {
			return false, nil
		}
	}

	for _, groupID := range c.localAssignedGroupIDs(assignments) {
		if _, err := c.LeaderOf(groupID); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (c *Cluster) localAssignedGroupIDs(assignments []controllermeta.GroupAssignment) []multiraft.GroupID {
	if c == nil {
		return nil
	}

	localNodeID := uint64(c.cfg.NodeID)
	groupIDs := make([]multiraft.GroupID, 0, len(assignments))
	for _, assignment := range assignments {
		if assignmentContainsPeer(assignment.DesiredPeers, localNodeID) {
			groupIDs = append(groupIDs, multiraft.GroupID(assignment.GroupID))
		}
	}
	return groupIDs
}

func (c *Cluster) controllerReportAddr() string {
	if c.server != nil && c.server.Listener() != nil {
		return c.server.Listener().Addr().String()
	}
	return c.cfg.ListenAddr
}

func (c *Cluster) setRuntimePeers(groupID multiraft.GroupID, peers []multiraft.NodeID) {
	if c == nil {
		return
	}

	c.runtimePeersMu.Lock()
	c.runtimePeers[groupID] = append([]multiraft.NodeID(nil), peers...)
	c.runtimePeersMu.Unlock()
}

func (c *Cluster) getRuntimePeers(groupID multiraft.GroupID) ([]multiraft.NodeID, bool) {
	if c == nil {
		return nil, false
	}

	c.runtimePeersMu.RLock()
	peers, ok := c.runtimePeers[groupID]
	c.runtimePeersMu.RUnlock()
	if !ok {
		return nil, false
	}
	return append([]multiraft.NodeID(nil), peers...), true
}

func (c *Cluster) deleteRuntimePeers(groupID multiraft.GroupID) {
	if c == nil {
		return
	}

	c.runtimePeersMu.Lock()
	delete(c.runtimePeers, groupID)
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
		if err := c.controller.Propose(proposeCtx, groupcontroller.Command{
			Kind:   groupcontroller.CommandKindNodeHeartbeat,
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
		advance := &groupcontroller.TaskAdvance{
			GroupID: req.Advance.GroupID,
			Attempt: req.Advance.Attempt,
			Now:     req.Advance.Now,
		}
		if req.Advance.Err != "" {
			advance.Err = errors.New(req.Advance.Err)
		}
		proposeCtx, cancel := withControllerTimeout(ctx)
		defer cancel()
		if err := c.controller.Propose(proposeCtx, groupcontroller.Command{
			Kind:    groupcontroller.CommandKindTaskResult,
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
		if err := c.controller.Propose(proposeCtx, groupcontroller.Command{
			Kind: groupcontroller.CommandKindOperatorRequest,
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
		task, err := c.controllerMeta.GetTask(ctx, req.GroupID)
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
		if err := c.forceReconcileOnLeader(ctx, req.GroupID); err != nil {
			return nil, err
		}
		return json.Marshal(controllerRPCResponse{})
	default:
		return nil, ErrInvalidConfig
	}
}
