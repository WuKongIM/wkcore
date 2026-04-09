package raftcluster

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/controllerraft"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/groupcontroller"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
)

type TaskStatus = controllermeta.TaskStatus

const (
	TaskStatusPending  = controllermeta.TaskStatusPending
	TaskStatusRetrying = controllermeta.TaskStatusRetrying
	TaskStatusFailed   = controllermeta.TaskStatusFailed
)

type RecoverStrategy uint8

const (
	RecoverStrategyLatestLiveReplica RecoverStrategy = iota + 1
	controllerLeaderWaitTimeout                      = 10 * time.Second
)

func (c *Cluster) MarkNodeDraining(ctx context.Context, nodeID uint64) error {
	if c.controllerClient == nil {
		return ErrNotStarted
	}
	return c.retryControllerCommand(ctx, func(attemptCtx context.Context) error {
		return c.controllerClient.Operator(attemptCtx, groupcontroller.OperatorRequest{
			Kind:   groupcontroller.OperatorMarkNodeDraining,
			NodeID: nodeID,
		})
	})
}

func (c *Cluster) ResumeNode(ctx context.Context, nodeID uint64) error {
	if c.controllerClient == nil {
		return ErrNotStarted
	}
	return c.retryControllerCommand(ctx, func(attemptCtx context.Context) error {
		return c.controllerClient.Operator(attemptCtx, groupcontroller.OperatorRequest{
			Kind:   groupcontroller.OperatorResumeNode,
			NodeID: nodeID,
		})
	})
}

func (c *Cluster) GetReconcileTask(ctx context.Context, groupID uint32) (controllermeta.ReconcileTask, error) {
	if c.controllerClient != nil {
		var task controllermeta.ReconcileTask
		err := c.retryControllerCommand(ctx, func(attemptCtx context.Context) error {
			var err error
			task, err = c.controllerClient.GetTask(attemptCtx, groupID)
			return err
		})
		if err == nil {
			return task, nil
		}
		if !controllerReadFallbackAllowed(err) || c.controllerMeta == nil {
			return controllermeta.ReconcileTask{}, err
		}
	}
	if c.controllerMeta != nil {
		return c.controllerMeta.GetTask(ctx, groupID)
	}
	return controllermeta.ReconcileTask{}, ErrNotStarted
}

func (c *Cluster) ForceReconcile(ctx context.Context, groupID uint32) error {
	if c.controllerClient != nil {
		return c.retryControllerCommand(ctx, func(attemptCtx context.Context) error {
			return c.controllerClient.ForceReconcile(attemptCtx, groupID)
		})
	}
	if c.controller != nil && c.controller.LeaderID() == uint64(c.cfg.NodeID) {
		return c.forceReconcileOnLeader(ctx, groupID)
	}
	return ErrNotStarted
}

func (c *Cluster) TransferGroupLeader(ctx context.Context, groupID uint32, nodeID multiraft.NodeID) error {
	return c.transferGroupLeadership(ctx, multiraft.GroupID(groupID), nodeID)
}

func (c *Cluster) RecoverGroup(ctx context.Context, groupID uint32, strategy RecoverStrategy) error {
	if strategy != RecoverStrategyLatestLiveReplica {
		return ErrInvalidConfig
	}

	assignments, err := c.ListGroupAssignments(ctx)
	if err != nil {
		return err
	}

	var peers []uint64
	for _, assignment := range assignments {
		if assignment.GroupID == groupID {
			peers = assignment.DesiredPeers
			break
		}
	}

	if len(peers) == 0 {
		return ErrGroupNotFound
	}

	reachable := 0
	for _, peer := range peers {
		if multiraft.NodeID(peer) == c.cfg.NodeID {
			if _, err := c.runtime.Status(multiraft.GroupID(groupID)); err == nil {
				reachable++
			}
			continue
		}

		body, err := json.Marshal(managedGroupRPCRequest{
			Kind:    managedGroupRPCStatus,
			GroupID: groupID,
		})
		if err != nil {
			return err
		}
		respBody, err := c.RPCService(ctx, multiraft.NodeID(peer), multiraft.GroupID(groupID), rpcServiceManagedGroup, body)
		if err != nil {
			continue
		}
		if decodeManagedGroupResponse(respBody) == nil {
			reachable++
		}
	}
	if reachable < len(peers)/2+1 {
		return ErrManualRecoveryRequired
	}
	return nil
}

func (c *Cluster) forceReconcileOnLeader(ctx context.Context, groupID uint32) error {
	if c.controller == nil || c.controllerMeta == nil {
		return ErrNotStarted
	}

	now := time.Now()
	assignment, assignmentErr := c.controllerMeta.GetAssignment(ctx, groupID)
	task, taskErr := c.controllerMeta.GetTask(ctx, groupID)

	switch {
	case taskErr == nil:
		task.Status = controllermeta.TaskStatusPending
		task.NextRunAt = now
		proposeCtx, cancel := withControllerTimeout(ctx)
		defer cancel()
		if err := c.controller.Propose(proposeCtx, groupcontroller.Command{
			Kind:       groupcontroller.CommandKindAssignmentTaskUpdate,
			Assignment: &assignment,
			Task:       &task,
		}); err != nil {
			if errors.Is(err, controllerraft.ErrNotLeader) {
				return ErrNotLeader
			}
			return err
		}
		return nil
	case !errors.Is(taskErr, controllermeta.ErrNotFound):
		return taskErr
	}

	state, err := c.snapshotPlannerState(ctx)
	if err != nil {
		return err
	}
	planner := groupcontroller.NewPlanner(groupcontroller.PlannerConfig{
		GroupCount: c.cfg.GroupCount,
		ReplicaN:   c.cfg.GroupReplicaN,
	})
	decision, err := planner.ReconcileGroup(ctx, state, groupID)
	if err != nil {
		return err
	}
	if decision.Task == nil {
		if errors.Is(assignmentErr, controllermeta.ErrNotFound) {
			return controllermeta.ErrNotFound
		}
		return nil
	}
	if decision.Task.Kind == controllermeta.TaskKindRebalance {
		return nil
	}

	proposeCtx, cancel := withControllerTimeout(ctx)
	defer cancel()
	if err := c.controller.Propose(proposeCtx, groupcontroller.Command{
		Kind:       groupcontroller.CommandKindAssignmentTaskUpdate,
		Assignment: &decision.Assignment,
		Task:       decision.Task,
	}); err != nil {
		if errors.Is(err, controllerraft.ErrNotLeader) {
			return ErrNotLeader
		}
		return err
	}
	return nil
}

func (c *Cluster) retryControllerCommand(ctx context.Context, fn func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	retryCtx := ctx
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		retryCtx, cancel = context.WithTimeout(ctx, controllerLeaderWaitTimeout)
		defer cancel()
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		err := fn(retryCtx)
		if err == nil {
			return nil
		}
		if !controllerCommandRetryAllowed(err) {
			return err
		}
		lastErr = err

		select {
		case <-retryCtx.Done():
			if lastErr != nil {
				return lastErr
			}
			return retryCtx.Err()
		case <-ticker.C:
		}
	}
}
