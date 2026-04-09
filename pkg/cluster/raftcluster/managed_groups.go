package raftcluster

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
	raft "go.etcd.io/raft/v3"
)

const (
	rpcServiceManagedGroup        uint8  = 20
	managedGroupRPCStatus         string = "status"
	managedGroupRPCChangeConfig   string = "change_config"
	managedGroupRPCTransferLeader string = "transfer_leader"
)

type managedGroupRPCRequest struct {
	Kind       string                `json:"kind"`
	GroupID    uint32                `json:"group_id"`
	TargetNode uint64                `json:"target_node,omitempty"`
	ChangeType multiraft.ChangeType  `json:"change_type,omitempty"`
	NodeID     uint64                `json:"node_id,omitempty"`
}

type managedGroupRPCResponse struct {
	NotLeader bool   `json:"not_leader,omitempty"`
	NotFound  bool   `json:"not_found,omitempty"`
	Timeout   bool   `json:"timeout,omitempty"`
	Message   string `json:"message,omitempty"`
}

type ManagedGroupExecutionTestHook func(groupID uint32, task controllermeta.ReconcileTask) error

var managedGroupExecutionTestHook struct {
	mu   sync.RWMutex
	hook ManagedGroupExecutionTestHook
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

func (c *Cluster) handleManagedGroupRPC(ctx context.Context, body []byte) ([]byte, error) {
	var req managedGroupRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	switch req.Kind {
	case managedGroupRPCStatus:
		_, err := c.runtime.Status(multiraft.GroupID(req.GroupID))
		switch {
		case err == nil:
			return json.Marshal(managedGroupRPCResponse{})
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

func (c *Cluster) ensureManagedGroupLocal(ctx context.Context, groupID multiraft.GroupID, desiredPeers []uint64, bootstrap bool) error {
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
	c.setRuntimePeers(groupID, peers)
	if bootstrap {
		if err := c.runtime.BootstrapGroup(ctx, multiraft.BootstrapGroupRequest{
			Group:  opts,
			Voters: peers,
		}); err != nil && !errors.Is(err, multiraft.ErrGroupExists) {
			return err
		}
		return nil
	}
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
		_, err := c.LeaderOf(groupID)
		return err
	case controllermeta.TaskKindRepair, controllermeta.TaskKindRebalance:
		if err := c.changeGroupConfig(ctx, groupID, multiraft.ConfigChange{
			Type:   multiraft.AddLearner,
			NodeID: multiraft.NodeID(assignment.task.TargetNode),
		}); err != nil {
			return err
		}
		if err := c.waitForManagedGroupOnNode(ctx, groupID, multiraft.NodeID(assignment.task.TargetNode)); err != nil {
			return err
		}
		if err := c.changeGroupConfig(ctx, groupID, multiraft.ConfigChange{
			Type:   multiraft.PromoteLearner,
			NodeID: multiraft.NodeID(assignment.task.TargetNode),
		}); err != nil {
			return err
		}
		if assignment.view.LeaderID == assignment.task.SourceNode && assignment.task.TargetNode != 0 {
			if err := c.transferGroupLeadership(ctx, groupID, multiraft.NodeID(assignment.task.TargetNode)); err != nil {
				return err
			}
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

func (c *Cluster) waitForManagedGroupOnNode(ctx context.Context, groupID multiraft.GroupID, nodeID multiraft.NodeID) error {
	deadline := time.Now().Add(2 * time.Second)
	for {
		if time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
		body, err := json.Marshal(managedGroupRPCRequest{
			Kind:    managedGroupRPCStatus,
			GroupID: uint32(groupID),
		})
		if err != nil {
			return err
		}
		respBody, err := c.RPCService(ctx, nodeID, groupID, rpcServiceManagedGroup, body)
		if err == nil && decodeManagedGroupResponse(respBody) == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
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
