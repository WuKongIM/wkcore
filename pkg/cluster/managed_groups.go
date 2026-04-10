package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	"github.com/WuKongIM/WuKongIM/pkg/group/multiraft"
	raft "go.etcd.io/raft/v3"
)

const (
	rpcServiceManagedGroup        uint8  = 20
	managedGroupRPCStatus         string = "status"
	managedGroupRPCChangeConfig   string = "change_config"
	managedGroupRPCTransferLeader string = "transfer_leader"
	managedGroupLeaderWaitTimeout        = 5 * time.Second
	managedGroupCatchUpTimeout           = 5 * time.Second
	managedGroupLeaderMoveTimeout        = 5 * time.Second
)

type managedGroupRPCRequest struct {
	Kind       string               `json:"kind"`
	GroupID    uint32               `json:"group_id"`
	TargetNode uint64               `json:"target_node,omitempty"`
	ChangeType multiraft.ChangeType `json:"change_type,omitempty"`
	NodeID     uint64               `json:"node_id,omitempty"`
}

type managedGroupRPCResponse struct {
	NotLeader    bool   `json:"not_leader,omitempty"`
	NotFound     bool   `json:"not_found,omitempty"`
	Timeout      bool   `json:"timeout,omitempty"`
	Message      string `json:"message,omitempty"`
	LeaderID     uint64 `json:"leader_id,omitempty"`
	CommitIndex  uint64 `json:"commit_index,omitempty"`
	AppliedIndex uint64 `json:"applied_index,omitempty"`
}

type ManagedGroupExecutionTestHook func(groupID uint32, task controllermeta.ReconcileTask) error

type managedGroupStatus struct {
	LeaderID     multiraft.NodeID
	CommitIndex  uint64
	AppliedIndex uint64
}

type managedGroupStatusTestHook func(c *Cluster, nodeID multiraft.NodeID, groupID multiraft.GroupID) (managedGroupStatus, error, bool)
type managedGroupLeaderTestHook func(c *Cluster, groupID multiraft.GroupID) (multiraft.NodeID, error, bool)

var managedGroupExecutionTestHook struct {
	mu   sync.RWMutex
	hook ManagedGroupExecutionTestHook
}

var managedGroupStatusHooks struct {
	mu   sync.RWMutex
	hook managedGroupStatusTestHook
}

var managedGroupLeaderHooks struct {
	mu   sync.RWMutex
	hook managedGroupLeaderTestHook
}

func SetManagedGroupExecutionTestHook(hook ManagedGroupExecutionTestHook) func() {
	managedGroupExecutionTestHook.mu.Lock()
	prev := managedGroupExecutionTestHook.hook
	managedGroupExecutionTestHook.hook = hook
	managedGroupExecutionTestHook.mu.Unlock()
	return func() {
		managedGroupExecutionTestHook.mu.Lock()
		managedGroupExecutionTestHook.hook = prev
		managedGroupExecutionTestHook.mu.Unlock()
	}
}

func setManagedGroupStatusTestHook(hook managedGroupStatusTestHook) func() {
	managedGroupStatusHooks.mu.Lock()
	prev := managedGroupStatusHooks.hook
	managedGroupStatusHooks.hook = hook
	managedGroupStatusHooks.mu.Unlock()
	return func() {
		managedGroupStatusHooks.mu.Lock()
		managedGroupStatusHooks.hook = prev
		managedGroupStatusHooks.mu.Unlock()
	}
}

func setManagedGroupLeaderTestHook(hook managedGroupLeaderTestHook) func() {
	managedGroupLeaderHooks.mu.Lock()
	prev := managedGroupLeaderHooks.hook
	managedGroupLeaderHooks.hook = hook
	managedGroupLeaderHooks.mu.Unlock()
	return func() {
		managedGroupLeaderHooks.mu.Lock()
		managedGroupLeaderHooks.hook = prev
		managedGroupLeaderHooks.mu.Unlock()
	}
}

func (c *Cluster) handleManagedGroupRPC(ctx context.Context, body []byte) ([]byte, error) {
	var req managedGroupRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	switch req.Kind {
	case managedGroupRPCStatus:
		status, err := c.runtime.Status(multiraft.GroupID(req.GroupID))
		switch {
		case err == nil:
			return json.Marshal(managedGroupRPCResponse{
				LeaderID:     uint64(status.LeaderID),
				CommitIndex:  status.CommitIndex,
				AppliedIndex: status.AppliedIndex,
			})
		case errors.Is(err, multiraft.ErrGroupNotFound):
			return json.Marshal(managedGroupRPCResponse{NotFound: true})
		default:
			return json.Marshal(managedGroupRPCResponse{Message: err.Error()})
		}
	case managedGroupRPCChangeConfig:
		err := c.changeGroupConfigLocal(ctx, multiraft.GroupID(req.GroupID), multiraft.ConfigChange{
			Type:   req.ChangeType,
			NodeID: multiraft.NodeID(req.NodeID),
		})
		return marshalManagedGroupError(err)
	case managedGroupRPCTransferLeader:
		err := c.transferGroupLeaderLocal(ctx, multiraft.GroupID(req.GroupID), multiraft.NodeID(req.TargetNode))
		return marshalManagedGroupError(err)
	default:
		return nil, ErrInvalidConfig
	}
}

func marshalManagedGroupError(err error) ([]byte, error) {
	switch {
	case err == nil:
		return json.Marshal(managedGroupRPCResponse{})
	case errors.Is(err, ErrNotLeader):
		return json.Marshal(managedGroupRPCResponse{NotLeader: true})
	case errors.Is(err, ErrGroupNotFound), errors.Is(err, multiraft.ErrGroupNotFound):
		return json.Marshal(managedGroupRPCResponse{NotFound: true})
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return json.Marshal(managedGroupRPCResponse{Timeout: true})
	default:
		return json.Marshal(managedGroupRPCResponse{Message: err.Error()})
	}
}

func (c *Cluster) ensureManagedGroupLocal(ctx context.Context, groupID multiraft.GroupID, desiredPeers []uint64, hasRuntimeView bool, bootstrapAuthorized bool) error {
	if c.runtime == nil {
		return ErrNotStarted
	}
	if _, err := c.runtime.Status(groupID); err == nil {
		c.setRuntimePeers(groupID, nodeIDsFromUint64s(desiredPeers))
		return nil
	} else if !errors.Is(err, multiraft.ErrGroupNotFound) {
		return err
	}

	storage, err := c.cfg.NewStorage(groupID)
	if err != nil {
		return err
	}
	sm, err := c.cfg.NewStateMachine(groupID)
	if err != nil {
		return err
	}
	opts := multiraft.GroupOptions{
		ID:           groupID,
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
		c.setRuntimePeers(groupID, peers)
		if err := c.runtime.OpenGroup(ctx, opts); err != nil && !errors.Is(err, multiraft.ErrGroupExists) {
			return err
		}
		return nil
	}

	peers := nodeIDsFromUint64s(desiredPeers)
	if bootstrapAuthorized {
		c.setRuntimePeers(groupID, peers)
		if err := c.runtime.BootstrapGroup(ctx, multiraft.BootstrapGroupRequest{
			Group:  opts,
			Voters: peers,
		}); err != nil && !errors.Is(err, multiraft.ErrGroupExists) {
			return err
		}
		return nil
	}
	if !hasRuntimeView {
		return nil
	}
	c.setRuntimePeers(groupID, peers)
	if err := c.runtime.OpenGroup(ctx, opts); err != nil && !errors.Is(err, multiraft.ErrGroupExists) {
		return err
	}
	return nil
}

func (c *Cluster) executeReconcileTask(ctx context.Context, assignment assignmentTaskState) error {
	groupID := multiraft.GroupID(assignment.assignment.GroupID)
	c.setRuntimePeers(groupID, nodeIDsFromUint64s(assignment.assignment.DesiredPeers))

	managedGroupExecutionTestHook.mu.RLock()
	hook := managedGroupExecutionTestHook.hook
	managedGroupExecutionTestHook.mu.RUnlock()
	if hook != nil {
		if err := hook(uint32(groupID), assignment.task); err != nil {
			return err
		}
	}

	switch assignment.task.Kind {
	case controllermeta.TaskKindBootstrap:
		return c.waitForManagedGroupLeader(ctx, groupID)
	case controllermeta.TaskKindRepair, controllermeta.TaskKindRebalance:
		if err := c.changeGroupConfig(ctx, groupID, multiraft.ConfigChange{
			Type:   multiraft.AddLearner,
			NodeID: multiraft.NodeID(assignment.task.TargetNode),
		}); err != nil {
			return err
		}
		if err := c.waitForManagedGroupCatchUp(ctx, groupID, multiraft.NodeID(assignment.task.TargetNode)); err != nil {
			return err
		}
		if err := c.changeGroupConfig(ctx, groupID, multiraft.ConfigChange{
			Type:   multiraft.PromoteLearner,
			NodeID: multiraft.NodeID(assignment.task.TargetNode),
		}); err != nil {
			return err
		}
		if err := c.waitForManagedGroupCatchUp(ctx, groupID, multiraft.NodeID(assignment.task.TargetNode)); err != nil {
			return err
		}
		if err := c.ensureLeaderMovedOffSource(
			ctx,
			groupID,
			multiraft.NodeID(assignment.task.SourceNode),
			multiraft.NodeID(assignment.task.TargetNode),
		); err != nil {
			return err
		}
		if assignment.task.SourceNode != 0 {
			if err := c.changeGroupConfig(ctx, groupID, multiraft.ConfigChange{
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

func (c *Cluster) changeGroupConfig(ctx context.Context, groupID multiraft.GroupID, change multiraft.ConfigChange) error {
	const retries = 3
	for attempt := 0; attempt < retries; attempt++ {
		leaderID, err := c.LeaderOf(groupID)
		if err != nil {
			return err
		}
		if c.IsLocal(leaderID) {
			err = c.changeGroupConfigLocal(ctx, groupID, change)
		} else {
			err = c.changeGroupConfigRemote(ctx, leaderID, groupID, change)
		}
		if !errors.Is(err, ErrNotLeader) {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ErrLeaderNotStable
}

func (c *Cluster) changeGroupConfigLocal(ctx context.Context, groupID multiraft.GroupID, change multiraft.ConfigChange) error {
	future, err := c.runtime.ChangeConfig(ctx, groupID, change)
	if err != nil {
		if errors.Is(err, multiraft.ErrGroupNotFound) {
			return ErrGroupNotFound
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
	if errors.Is(err, multiraft.ErrGroupNotFound) {
		return ErrGroupNotFound
	}
	return err
}

func (c *Cluster) changeGroupConfigRemote(ctx context.Context, leaderID multiraft.NodeID, groupID multiraft.GroupID, change multiraft.ConfigChange) error {
	body, err := json.Marshal(managedGroupRPCRequest{
		Kind:       managedGroupRPCChangeConfig,
		GroupID:    uint32(groupID),
		ChangeType: change.Type,
		NodeID:     uint64(change.NodeID),
	})
	if err != nil {
		return err
	}
	respBody, err := c.RPCService(ctx, leaderID, groupID, rpcServiceManagedGroup, body)
	if err != nil {
		return err
	}
	return decodeManagedGroupResponse(respBody)
}

func (c *Cluster) transferGroupLeadership(ctx context.Context, groupID multiraft.GroupID, target multiraft.NodeID) error {
	const retries = 3
	for attempt := 0; attempt < retries; attempt++ {
		leaderID, err := c.LeaderOf(groupID)
		if err != nil {
			return err
		}
		if c.IsLocal(leaderID) {
			err = c.transferGroupLeaderLocal(ctx, groupID, target)
		} else {
			err = c.transferGroupLeaderRemote(ctx, leaderID, groupID, target)
		}
		if !errors.Is(err, ErrNotLeader) {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ErrLeaderNotStable
}

func (c *Cluster) transferGroupLeaderLocal(ctx context.Context, groupID multiraft.GroupID, target multiraft.NodeID) error {
	err := c.runtime.TransferLeadership(ctx, groupID, target)
	if errors.Is(err, multiraft.ErrGroupNotFound) {
		return ErrGroupNotFound
	}
	return err
}

func (c *Cluster) transferGroupLeaderRemote(ctx context.Context, leaderID multiraft.NodeID, groupID multiraft.GroupID, target multiraft.NodeID) error {
	body, err := json.Marshal(managedGroupRPCRequest{
		Kind:       managedGroupRPCTransferLeader,
		GroupID:    uint32(groupID),
		TargetNode: uint64(target),
	})
	if err != nil {
		return err
	}
	respBody, err := c.RPCService(ctx, leaderID, groupID, rpcServiceManagedGroup, body)
	if err != nil {
		return err
	}
	return decodeManagedGroupResponse(respBody)
}

func (c *Cluster) waitForManagedGroupLeader(ctx context.Context, groupID multiraft.GroupID) error {
	deadline := time.Now().Add(managedGroupLeaderWaitTimeout)
	for {
		if time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
		if _, err := c.LeaderOf(groupID); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (c *Cluster) waitForManagedGroupCatchUp(ctx context.Context, groupID multiraft.GroupID, targetNode multiraft.NodeID) error {
	deadline := time.Now().Add(managedGroupCatchUpTimeout)
	for {
		if time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
		targetStatus, err := c.managedGroupStatusOnNode(ctx, targetNode, groupID)
		if err == nil {
			leaderID, leaderErr := c.currentManagedGroupLeader(groupID)
			if leaderErr == nil && leaderID != 0 {
				leaderStatus, statusErr := c.managedGroupStatusOnNode(ctx, leaderID, groupID)
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

func (c *Cluster) currentManagedGroupLeader(groupID multiraft.GroupID) (multiraft.NodeID, error) {
	managedGroupLeaderHooks.mu.RLock()
	hook := managedGroupLeaderHooks.hook
	managedGroupLeaderHooks.mu.RUnlock()
	if hook != nil {
		if leaderID, err, handled := hook(c, groupID); handled {
			return leaderID, err
		}
	}
	return c.LeaderOf(groupID)
}

func (c *Cluster) ensureLeaderMovedOffSource(ctx context.Context, groupID multiraft.GroupID, sourceNode, targetNode multiraft.NodeID) error {
	if sourceNode == 0 || targetNode == 0 {
		return nil
	}
	deadline := time.Now().Add(managedGroupLeaderMoveTimeout)
	for {
		if time.Now().After(deadline) {
			return ErrLeaderNotStable
		}
		leaderID, err := c.LeaderOf(groupID)
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
		if err := c.transferGroupLeadership(ctx, groupID, targetNode); err != nil && !errors.Is(err, ErrNotLeader) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (c *Cluster) managedGroupStatusOnNode(ctx context.Context, nodeID multiraft.NodeID, groupID multiraft.GroupID) (managedGroupStatus, error) {
	managedGroupStatusHooks.mu.RLock()
	hook := managedGroupStatusHooks.hook
	managedGroupStatusHooks.mu.RUnlock()
	if hook != nil {
		if status, err, handled := hook(c, nodeID, groupID); handled {
			return status, err
		}
	}

	if c.IsLocal(nodeID) {
		return c.localManagedGroupStatus(groupID)
	}

	body, err := json.Marshal(managedGroupRPCRequest{
		Kind:    managedGroupRPCStatus,
		GroupID: uint32(groupID),
	})
	if err != nil {
		return managedGroupStatus{}, err
	}
	respBody, err := c.RPCService(ctx, nodeID, groupID, rpcServiceManagedGroup, body)
	if err != nil {
		return managedGroupStatus{}, err
	}
	var resp managedGroupRPCResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return managedGroupStatus{}, err
	}
	if err := decodeManagedGroupResponse(respBody); err != nil {
		return managedGroupStatus{}, err
	}
	return managedGroupStatus{
		LeaderID:     multiraft.NodeID(resp.LeaderID),
		CommitIndex:  resp.CommitIndex,
		AppliedIndex: resp.AppliedIndex,
	}, nil
}

func (c *Cluster) localManagedGroupStatus(groupID multiraft.GroupID) (managedGroupStatus, error) {
	if c.runtime == nil {
		return managedGroupStatus{}, ErrNotStarted
	}
	status, err := c.runtime.Status(groupID)
	if err != nil {
		if errors.Is(err, multiraft.ErrGroupNotFound) {
			return managedGroupStatus{}, ErrGroupNotFound
		}
		return managedGroupStatus{}, err
	}
	return managedGroupStatus{
		LeaderID:     status.LeaderID,
		CommitIndex:  status.CommitIndex,
		AppliedIndex: status.AppliedIndex,
	}, nil
}

func decodeManagedGroupResponse(body []byte) error {
	var resp managedGroupRPCResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return err
	}
	switch {
	case resp.NotLeader:
		return ErrNotLeader
	case resp.NotFound:
		return ErrGroupNotFound
	case resp.Timeout:
		return context.DeadlineExceeded
	case resp.Message != "":
		return errors.New(resp.Message)
	default:
		return nil
	}
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
