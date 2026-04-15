package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	metadb "github.com/WuKongIM/WuKongIM/pkg/slot/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	raft "go.etcd.io/raft/v3"
)

const (
	rpcServiceManagedSlot        uint8  = 20
	managedSlotRPCStatus         string = "status"
	managedSlotRPCChangeConfig   string = "change_config"
	managedSlotRPCImportSnapshot string = "import_snapshot"
	managedSlotRPCTransferLeader string = "transfer_leader"
	managedSlotLeaderWaitTimeout        = 5 * time.Second
	managedSlotCatchUpTimeout           = 5 * time.Second
	managedSlotLeaderMoveTimeout        = 5 * time.Second
)

type managedSlotRPCRequest struct {
	Kind       string               `json:"kind"`
	SlotID     uint32               `json:"slot_id"`
	TargetNode uint64               `json:"target_node,omitempty"`
	ChangeType multiraft.ChangeType `json:"change_type,omitempty"`
	NodeID     uint64               `json:"node_id,omitempty"`
	HashSlot   uint16               `json:"hash_slot,omitempty"`
	Snapshot   []byte               `json:"snapshot,omitempty"`
}

type managedSlotRPCResponse struct {
	NotLeader    bool   `json:"not_leader,omitempty"`
	NotFound     bool   `json:"not_found,omitempty"`
	Timeout      bool   `json:"timeout,omitempty"`
	Message      string `json:"message,omitempty"`
	LeaderID     uint64 `json:"leader_id,omitempty"`
	CommitIndex  uint64 `json:"commit_index,omitempty"`
	AppliedIndex uint64 `json:"applied_index,omitempty"`
}

type ManagedSlotExecutionTestHook func(slotID uint32, task controllermeta.ReconcileTask) error

type managedSlotStatus struct {
	LeaderID     multiraft.NodeID
	CommitIndex  uint64
	AppliedIndex uint64
}

type managedSlotStatusTestHook func(c *Cluster, nodeID multiraft.NodeID, slotID multiraft.SlotID) (managedSlotStatus, error, bool)
type managedSlotLeaderTestHook func(c *Cluster, slotID multiraft.SlotID) (multiraft.NodeID, error, bool)

var managedSlotExecutionTestHook struct {
	mu   sync.RWMutex
	hook ManagedSlotExecutionTestHook
}

var managedSlotStatusHooks struct {
	mu   sync.RWMutex
	hook managedSlotStatusTestHook
}

var managedSlotLeaderHooks struct {
	mu   sync.RWMutex
	hook managedSlotLeaderTestHook
}

func SetManagedSlotExecutionTestHook(hook ManagedSlotExecutionTestHook) func() {
	managedSlotExecutionTestHook.mu.Lock()
	prev := managedSlotExecutionTestHook.hook
	managedSlotExecutionTestHook.hook = hook
	managedSlotExecutionTestHook.mu.Unlock()
	return func() {
		managedSlotExecutionTestHook.mu.Lock()
		managedSlotExecutionTestHook.hook = prev
		managedSlotExecutionTestHook.mu.Unlock()
	}
}

func setManagedSlotStatusTestHook(hook managedSlotStatusTestHook) func() {
	managedSlotStatusHooks.mu.Lock()
	prev := managedSlotStatusHooks.hook
	managedSlotStatusHooks.hook = hook
	managedSlotStatusHooks.mu.Unlock()
	return func() {
		managedSlotStatusHooks.mu.Lock()
		managedSlotStatusHooks.hook = prev
		managedSlotStatusHooks.mu.Unlock()
	}
}

func setManagedSlotLeaderTestHook(hook managedSlotLeaderTestHook) func() {
	managedSlotLeaderHooks.mu.Lock()
	prev := managedSlotLeaderHooks.hook
	managedSlotLeaderHooks.hook = hook
	managedSlotLeaderHooks.mu.Unlock()
	return func() {
		managedSlotLeaderHooks.mu.Lock()
		managedSlotLeaderHooks.hook = prev
		managedSlotLeaderHooks.mu.Unlock()
	}
}

func (c *Cluster) handleManagedSlotRPC(ctx context.Context, body []byte) ([]byte, error) {
	var req managedSlotRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	switch req.Kind {
	case managedSlotRPCStatus:
		status, err := c.runtime.Status(multiraft.SlotID(req.SlotID))
		switch {
		case err == nil:
			return json.Marshal(managedSlotRPCResponse{
				LeaderID:     uint64(status.LeaderID),
				CommitIndex:  status.CommitIndex,
				AppliedIndex: status.AppliedIndex,
			})
		case errors.Is(err, multiraft.ErrSlotNotFound):
			return json.Marshal(managedSlotRPCResponse{NotFound: true})
		default:
			return json.Marshal(managedSlotRPCResponse{Message: err.Error()})
		}
	case managedSlotRPCChangeConfig:
		err := c.changeSlotConfigLocal(ctx, multiraft.SlotID(req.SlotID), multiraft.ConfigChange{
			Type:   req.ChangeType,
			NodeID: multiraft.NodeID(req.NodeID),
		})
		return marshalManagedSlotError(err)
	case managedSlotRPCTransferLeader:
		err := c.transferSlotLeaderLocal(ctx, multiraft.SlotID(req.SlotID), multiraft.NodeID(req.TargetNode))
		return marshalManagedSlotError(err)
	case managedSlotRPCImportSnapshot:
		leaderID, leaderErr := c.currentManagedSlotLeader(multiraft.SlotID(req.SlotID))
		if leaderErr != nil {
			return marshalManagedSlotError(leaderErr)
		}
		if !c.IsLocal(leaderID) {
			return marshalManagedSlotError(ErrNotLeader)
		}
		err := c.importHashSlotSnapshotLocal(ctx, multiraft.SlotID(req.SlotID), metadb.SlotSnapshot{
			HashSlots: []uint16{req.HashSlot},
			Data:      append([]byte(nil), req.Snapshot...),
		})
		return marshalManagedSlotError(err)
	default:
		return nil, ErrInvalidConfig
	}
}

func marshalManagedSlotError(err error) ([]byte, error) {
	switch {
	case err == nil:
		return json.Marshal(managedSlotRPCResponse{})
	case errors.Is(err, ErrNotLeader):
		return json.Marshal(managedSlotRPCResponse{NotLeader: true})
	case errors.Is(err, ErrSlotNotFound), errors.Is(err, multiraft.ErrSlotNotFound):
		return json.Marshal(managedSlotRPCResponse{NotFound: true})
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return json.Marshal(managedSlotRPCResponse{Timeout: true})
	default:
		return json.Marshal(managedSlotRPCResponse{Message: err.Error()})
	}
}

func (c *Cluster) ensureManagedSlotLocal(ctx context.Context, slotID multiraft.SlotID, desiredPeers []uint64, hasRuntimeView bool, bootstrapAuthorized bool) error {
	if c.runtime == nil {
		return ErrNotStarted
	}
	if _, err := c.runtime.Status(slotID); err == nil {
		c.setRuntimePeers(slotID, nodeIDsFromUint64s(desiredPeers))
		return nil
	} else if !errors.Is(err, multiraft.ErrSlotNotFound) {
		return err
	}

	storage, err := c.cfg.NewStorage(slotID)
	if err != nil {
		return err
	}
	sm, err := c.newStateMachine(slotID)
	if err != nil {
		return err
	}
	opts := multiraft.SlotOptions{
		ID:           slotID,
		Storage:      storage,
		StateMachine: sm,
	}

	initialState, err := storage.InitialState(ctx)
	if err != nil {
		return err
	}
	if !raft.IsEmptyHardState(initialState.HardState) {
		peers := nodeIDsFromUint64s(initialState.ConfState.Voters)
		if len(peers) == 0 {
			peers = nodeIDsFromUint64s(desiredPeers)
		}
		c.setRuntimePeers(slotID, peers)
		if err := c.runtime.OpenSlot(ctx, opts); err != nil && !errors.Is(err, multiraft.ErrSlotExists) {
			return err
		}
		return nil
	}

	peers := nodeIDsFromUint64s(desiredPeers)
	if bootstrapAuthorized {
		c.setRuntimePeers(slotID, peers)
		if err := c.runtime.BootstrapSlot(ctx, multiraft.BootstrapSlotRequest{
			Slot:   opts,
			Voters: peers,
		}); err != nil && !errors.Is(err, multiraft.ErrSlotExists) {
			return err
		}
		return nil
	}
	if !hasRuntimeView {
		return nil
	}
	c.setRuntimePeers(slotID, peers)
	if err := c.runtime.OpenSlot(ctx, opts); err != nil && !errors.Is(err, multiraft.ErrSlotExists) {
		return err
	}
	return nil
}

func (c *Cluster) executeReconcileTask(ctx context.Context, assignment assignmentTaskState) error {
	slotID := multiraft.SlotID(assignment.assignment.SlotID)
	c.setRuntimePeers(slotID, nodeIDsFromUint64s(assignment.assignment.DesiredPeers))

	managedSlotExecutionTestHook.mu.RLock()
	hook := managedSlotExecutionTestHook.hook
	managedSlotExecutionTestHook.mu.RUnlock()
	if hook != nil {
		if err := hook(uint32(slotID), assignment.task); err != nil {
			return err
		}
	}

	switch assignment.task.Kind {
	case controllermeta.TaskKindBootstrap:
		return c.waitForManagedSlotLeader(ctx, slotID)
	case controllermeta.TaskKindRepair, controllermeta.TaskKindRebalance:
		if err := c.changeSlotConfig(ctx, slotID, multiraft.ConfigChange{
			Type:   multiraft.AddLearner,
			NodeID: multiraft.NodeID(assignment.task.TargetNode),
		}); err != nil {
			return err
		}
		if err := c.waitForManagedSlotCatchUp(ctx, slotID, multiraft.NodeID(assignment.task.TargetNode)); err != nil {
			return err
		}
		if err := c.changeSlotConfig(ctx, slotID, multiraft.ConfigChange{
			Type:   multiraft.PromoteLearner,
			NodeID: multiraft.NodeID(assignment.task.TargetNode),
		}); err != nil {
			return err
		}
		if err := c.waitForManagedSlotCatchUp(ctx, slotID, multiraft.NodeID(assignment.task.TargetNode)); err != nil {
			return err
		}
		if err := c.ensureLeaderMovedOffSource(
			ctx,
			slotID,
			multiraft.NodeID(assignment.task.SourceNode),
			multiraft.NodeID(assignment.task.TargetNode),
		); err != nil {
			return err
		}
		if assignment.task.SourceNode != 0 {
			if err := c.changeSlotConfig(ctx, slotID, multiraft.ConfigChange{
				Type:   multiraft.RemoveVoter,
				NodeID: multiraft.NodeID(assignment.task.SourceNode),
			}); err != nil {
				return err
			}
		}
		return nil
	default:
		return nil
	}
}

func (c *Cluster) changeSlotConfig(ctx context.Context, slotID multiraft.SlotID, change multiraft.ConfigChange) error {
	const retries = 3
	for attempt := 0; attempt < retries; attempt++ {
		leaderID, err := c.LeaderOf(slotID)
		if err != nil {
			return err
		}
		if c.IsLocal(leaderID) {
			err = c.changeSlotConfigLocal(ctx, slotID, change)
		} else {
			err = c.changeSlotConfigRemote(ctx, leaderID, slotID, change)
		}
		if !errors.Is(err, ErrNotLeader) {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ErrLeaderNotStable
}

func (c *Cluster) changeSlotConfigLocal(ctx context.Context, slotID multiraft.SlotID, change multiraft.ConfigChange) error {
	future, err := c.runtime.ChangeConfig(ctx, slotID, change)
	if err != nil {
		if errors.Is(err, multiraft.ErrSlotNotFound) {
			return ErrSlotNotFound
		}
		if errors.Is(err, multiraft.ErrNotLeader) {
			return ErrNotLeader
		}
		return err
	}
	_, err = future.Wait(ctx)
	if errors.Is(err, multiraft.ErrNotLeader) {
		return ErrNotLeader
	}
	if errors.Is(err, multiraft.ErrSlotNotFound) {
		return ErrSlotNotFound
	}
	return err
}

func (c *Cluster) changeSlotConfigRemote(ctx context.Context, leaderID multiraft.NodeID, slotID multiraft.SlotID, change multiraft.ConfigChange) error {
	body, err := json.Marshal(managedSlotRPCRequest{
		Kind:       managedSlotRPCChangeConfig,
		SlotID:     uint32(slotID),
		ChangeType: change.Type,
		NodeID:     uint64(change.NodeID),
	})
	if err != nil {
		return err
	}
	respBody, err := c.RPCService(ctx, leaderID, slotID, rpcServiceManagedSlot, body)
	if err != nil {
		return err
	}
	return decodeManagedSlotResponse(respBody)
}

func (c *Cluster) transferSlotLeadership(ctx context.Context, slotID multiraft.SlotID, target multiraft.NodeID) error {
	const retries = 3
	for attempt := 0; attempt < retries; attempt++ {
		leaderID, err := c.LeaderOf(slotID)
		if err != nil {
			return err
		}
		if c.IsLocal(leaderID) {
			err = c.transferSlotLeaderLocal(ctx, slotID, target)
		} else {
			err = c.transferSlotLeaderRemote(ctx, leaderID, slotID, target)
		}
		if !errors.Is(err, ErrNotLeader) {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ErrLeaderNotStable
}

func (c *Cluster) transferSlotLeaderLocal(ctx context.Context, slotID multiraft.SlotID, target multiraft.NodeID) error {
	err := c.runtime.TransferLeadership(ctx, slotID, target)
	if errors.Is(err, multiraft.ErrSlotNotFound) {
		return ErrSlotNotFound
	}
	return err
}

func (c *Cluster) transferSlotLeaderRemote(ctx context.Context, leaderID multiraft.NodeID, slotID multiraft.SlotID, target multiraft.NodeID) error {
	body, err := json.Marshal(managedSlotRPCRequest{
		Kind:       managedSlotRPCTransferLeader,
		SlotID:     uint32(slotID),
		TargetNode: uint64(target),
	})
	if err != nil {
		return err
	}
	respBody, err := c.RPCService(ctx, leaderID, slotID, rpcServiceManagedSlot, body)
	if err != nil {
		return err
	}
	return decodeManagedSlotResponse(respBody)
}

func (c *Cluster) waitForManagedSlotLeader(ctx context.Context, slotID multiraft.SlotID) error {
	deadline := time.Now().Add(managedSlotLeaderWaitTimeout)
	for {
		if time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
		if _, err := c.LeaderOf(slotID); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (c *Cluster) waitForManagedSlotCatchUp(ctx context.Context, slotID multiraft.SlotID, targetNode multiraft.NodeID) error {
	deadline := time.Now().Add(managedSlotCatchUpTimeout)
	for {
		if time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
		targetStatus, err := c.managedSlotStatusOnNode(ctx, targetNode, slotID)
		if err == nil {
			leaderID, leaderErr := c.currentManagedSlotLeader(slotID)
			if leaderErr == nil && leaderID != 0 {
				leaderStatus, statusErr := c.managedSlotStatusOnNode(ctx, leaderID, slotID)
				if statusErr == nil && targetStatus.AppliedIndex >= leaderStatus.CommitIndex {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (c *Cluster) currentManagedSlotLeader(slotID multiraft.SlotID) (multiraft.NodeID, error) {
	managedSlotLeaderHooks.mu.RLock()
	hook := managedSlotLeaderHooks.hook
	managedSlotLeaderHooks.mu.RUnlock()
	if hook != nil {
		if leaderID, err, handled := hook(c, slotID); handled {
			return leaderID, err
		}
	}
	return c.LeaderOf(slotID)
}

func (c *Cluster) ensureLeaderMovedOffSource(ctx context.Context, slotID multiraft.SlotID, sourceNode, targetNode multiraft.NodeID) error {
	if sourceNode == 0 || targetNode == 0 {
		return nil
	}
	deadline := time.Now().Add(managedSlotLeaderMoveTimeout)
	for {
		if time.Now().After(deadline) {
			return ErrLeaderNotStable
		}
		leaderID, err := c.LeaderOf(slotID)
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		if leaderID != sourceNode {
			return nil
		}
		if err := c.transferSlotLeadership(ctx, slotID, targetNode); err != nil && !errors.Is(err, ErrNotLeader) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (c *Cluster) managedSlotStatusOnNode(ctx context.Context, nodeID multiraft.NodeID, slotID multiraft.SlotID) (managedSlotStatus, error) {
	managedSlotStatusHooks.mu.RLock()
	hook := managedSlotStatusHooks.hook
	managedSlotStatusHooks.mu.RUnlock()
	if hook != nil {
		if status, err, handled := hook(c, nodeID, slotID); handled {
			return status, err
		}
	}

	if c.IsLocal(nodeID) {
		return c.localManagedSlotStatus(slotID)
	}

	body, err := json.Marshal(managedSlotRPCRequest{
		Kind:   managedSlotRPCStatus,
		SlotID: uint32(slotID),
	})
	if err != nil {
		return managedSlotStatus{}, err
	}
	respBody, err := c.RPCService(ctx, nodeID, slotID, rpcServiceManagedSlot, body)
	if err != nil {
		return managedSlotStatus{}, err
	}
	var resp managedSlotRPCResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return managedSlotStatus{}, err
	}
	if err := decodeManagedSlotResponse(respBody); err != nil {
		return managedSlotStatus{}, err
	}
	return managedSlotStatus{
		LeaderID:     multiraft.NodeID(resp.LeaderID),
		CommitIndex:  resp.CommitIndex,
		AppliedIndex: resp.AppliedIndex,
	}, nil
}

func (c *Cluster) localManagedSlotStatus(slotID multiraft.SlotID) (managedSlotStatus, error) {
	if c.runtime == nil {
		return managedSlotStatus{}, ErrNotStarted
	}
	status, err := c.runtime.Status(slotID)
	if err != nil {
		if errors.Is(err, multiraft.ErrSlotNotFound) {
			return managedSlotStatus{}, ErrSlotNotFound
		}
		return managedSlotStatus{}, err
	}
	return managedSlotStatus{
		LeaderID:     status.LeaderID,
		CommitIndex:  status.CommitIndex,
		AppliedIndex: status.AppliedIndex,
	}, nil
}

func decodeManagedSlotResponse(body []byte) error {
	var resp managedSlotRPCResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return err
	}
	switch {
	case resp.NotLeader:
		return ErrNotLeader
	case resp.NotFound:
		return ErrSlotNotFound
	case resp.Timeout:
		return context.DeadlineExceeded
	case resp.Message != "":
		return errors.New(resp.Message)
	default:
		return nil
	}
}

func cloneSlotSnapshot(snap metadb.SlotSnapshot) metadb.SlotSnapshot {
	return metadb.SlotSnapshot{
		HashSlots: append([]uint16(nil), snap.HashSlots...),
		Data:      append([]byte(nil), snap.Data...),
		Stats:     snap.Stats,
	}
}

func (c *Cluster) exportHashSlotSnapshot(ctx context.Context, slotID multiraft.SlotID, hashSlot uint16) (metadb.SlotSnapshot, uint64, error) {
	sm, ok := c.runtimeStateMachine(slotID)
	if !ok {
		return metadb.SlotSnapshot{}, 0, ErrSlotNotFound
	}
	exporter, ok := sm.(hashSlotSnapshotExporter)
	if !ok {
		return metadb.SlotSnapshot{}, 0, ErrInvalidConfig
	}

	status, err := c.managedSlotStatusOnNode(ctx, c.cfg.NodeID, slotID)
	if err != nil {
		return metadb.SlotSnapshot{}, 0, err
	}

	snap, err := exporter.ExportHashSlotSnapshot(ctx, hashSlot)
	if err != nil {
		return metadb.SlotSnapshot{}, 0, err
	}
	return cloneSlotSnapshot(snap), status.AppliedIndex, nil
}

func (c *Cluster) importHashSlotSnapshot(ctx context.Context, slotID multiraft.SlotID, snap metadb.SlotSnapshot) error {
	leaderID, err := c.currentManagedSlotLeader(slotID)
	if err != nil {
		return err
	}
	if c.IsLocal(leaderID) {
		return c.importHashSlotSnapshotLocal(ctx, slotID, snap)
	}
	return c.importHashSlotSnapshotRemote(ctx, leaderID, slotID, snap)
}

func (c *Cluster) importHashSlotSnapshotLocal(ctx context.Context, slotID multiraft.SlotID, snap metadb.SlotSnapshot) error {
	sm, ok := c.runtimeStateMachine(slotID)
	if !ok {
		return ErrSlotNotFound
	}
	importer, ok := sm.(hashSlotSnapshotImporter)
	if !ok {
		return ErrInvalidConfig
	}
	return importer.ImportHashSlotSnapshot(ctx, cloneSlotSnapshot(snap))
}

func (c *Cluster) importHashSlotSnapshotRemote(ctx context.Context, leaderID multiraft.NodeID, slotID multiraft.SlotID, snap metadb.SlotSnapshot) error {
	if len(snap.HashSlots) != 1 {
		return ErrInvalidConfig
	}
	body, err := json.Marshal(managedSlotRPCRequest{
		Kind:     managedSlotRPCImportSnapshot,
		SlotID:   uint32(slotID),
		HashSlot: snap.HashSlots[0],
		Snapshot: append([]byte(nil), snap.Data...),
	})
	if err != nil {
		return err
	}
	respBody, err := c.RPCService(ctx, leaderID, slotID, rpcServiceManagedSlot, body)
	if err != nil {
		return err
	}
	return decodeManagedSlotResponse(respBody)
}

func assignmentContainsPeer(peers []uint64, nodeID uint64) bool {
	for _, peer := range peers {
		if peer == nodeID {
			return true
		}
	}
	return false
}

func reconcileTaskRunnable(now time.Time, task controllermeta.ReconcileTask) bool {
	switch task.Status {
	case controllermeta.TaskStatusPending:
		return true
	case controllermeta.TaskStatusRetrying:
		return !task.NextRunAt.After(now)
	default:
		return false
	}
}
