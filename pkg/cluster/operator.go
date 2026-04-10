package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
	controllerraft "github.com/WuKongIM/WuKongIM/pkg/controller/raft"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

type TaskStatus = controllermeta.TaskStatus

const (
	TaskStatusPending  = controllermeta.TaskStatusPending
	TaskStatusRetrying = controllermeta.TaskStatusRetrying
	TaskStatusFailed   = controllermeta.TaskStatusFailed
)

type RecoverStrategy uint8

const (
	RecoverStrategyLatestLiveReplica   RecoverStrategy = iota + 1
	defaultControllerLeaderWaitTimeout                 = 10 * time.Second
)

func (c *Cluster) controllerLeaderWaitDuration() time.Duration {
	if c != nil && c.controllerLeaderWaitTimeout > 0 {
		return c.controllerLeaderWaitTimeout
	}
	return defaultControllerLeaderWaitTimeout
}

func (c *Cluster) MarkNodeDraining(ctx context.Context, nodeID uint64) error {
	if c.controllerClient == nil {
		return ErrNotStarted
	}
	return c.retryControllerCommand(ctx, func(attemptCtx context.Context) error {
		return c.controllerClient.Operator(attemptCtx, slotcontroller.OperatorRequest{
			Kind:   slotcontroller.OperatorMarkNodeDraining,
			NodeID: nodeID,
		})
	})
}

func (c *Cluster) ResumeNode(ctx context.Context, nodeID uint64) error {
	if c.controllerClient == nil {
		return ErrNotStarted
	}
	return c.retryControllerCommand(ctx, func(attemptCtx context.Context) error {
		return c.controllerClient.Operator(attemptCtx, slotcontroller.OperatorRequest{
			Kind:   slotcontroller.OperatorResumeNode,
			NodeID: nodeID,
		})
	})
}

func (c *Cluster) GetReconcileTask(ctx context.Context, slotID uint32) (controllermeta.ReconcileTask, error) {
	if c.controllerClient != nil {
		var task controllermeta.ReconcileTask
		err := c.retryControllerCommand(ctx, func(attemptCtx context.Context) error {
			var err error
			task, err = c.controllerClient.GetTask(attemptCtx, slotID)
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
		return c.controllerMeta.GetTask(ctx, slotID)
	}
	return controllermeta.ReconcileTask{}, ErrNotStarted
}

func (c *Cluster) ForceReconcile(ctx context.Context, slotID uint32) error {
	if c.controllerClient != nil {
		return c.retryControllerCommand(ctx, func(attemptCtx context.Context) error {
			return c.controllerClient.ForceReconcile(attemptCtx, slotID)
		})
	}
	if c.controller != nil && c.controller.LeaderID() == uint64(c.cfg.NodeID) {
		return c.forceReconcileOnLeader(ctx, slotID)
	}
	return ErrNotStarted
}

func (c *Cluster) TransferSlotLeader(ctx context.Context, slotID uint32, nodeID multiraft.NodeID) error {
	return c.transferSlotLeadership(ctx, multiraft.SlotID(slotID), nodeID)
}

func (c *Cluster) RecoverSlot(ctx context.Context, slotID uint32, strategy RecoverStrategy) error {
	if strategy != RecoverStrategyLatestLiveReplica {
		return ErrInvalidConfig
	}

	assignments, err := c.ListSlotAssignments(ctx)
	if err != nil {
		return err
	}

	var peers []uint64
	for _, assignment := range assignments {
		if assignment.SlotID == slotID {
			peers = assignment.DesiredPeers
			break
		}
	}

	if len(peers) == 0 {
		return ErrSlotNotFound
	}

	reachable := 0
	for _, peer := range peers {
		if multiraft.NodeID(peer) == c.cfg.NodeID {
			if _, err := c.runtime.Status(multiraft.SlotID(slotID)); err == nil {
				reachable++
			}
			continue
		}

		body, err := json.Marshal(managedSlotRPCRequest{
			Kind:   managedSlotRPCStatus,
			SlotID: slotID,
		})
		if err != nil {
			return err
		}
		respBody, err := c.RPCService(ctx, multiraft.NodeID(peer), multiraft.SlotID(slotID), rpcServiceManagedSlot, body)
		if err != nil {
			continue
		}
		if decodeManagedSlotResponse(respBody) == nil {
			reachable++
		}
	}
	if reachable < len(peers)/2+1 {
		return ErrManualRecoveryRequired
	}
	return nil
}

func (c *Cluster) forceReconcileOnLeader(ctx context.Context, slotID uint32) error {
	if c.controller == nil || c.controllerMeta == nil {
		return ErrNotStarted
	}

	now := time.Now()
	assignment, assignmentErr := c.controllerMeta.GetAssignment(ctx, slotID)
	task, taskErr := c.controllerMeta.GetTask(ctx, slotID)

	switch {
	case taskErr == nil:
		task.Status = controllermeta.TaskStatusPending
		task.NextRunAt = now
		proposeCtx, cancel := withControllerTimeout(ctx)
		defer cancel()
		if err := c.controller.Propose(proposeCtx, slotcontroller.Command{
			Kind:       slotcontroller.CommandKindAssignmentTaskUpdate,
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
	planner := slotcontroller.NewPlanner(slotcontroller.PlannerConfig{
		SlotCount: c.cfg.SlotCount,
		ReplicaN:  c.cfg.SlotReplicaN,
	})
	decision, err := planner.ReconcileSlot(ctx, state, slotID)
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
	if err := c.controller.Propose(proposeCtx, slotcontroller.Command{
		Kind:       slotcontroller.CommandKindAssignmentTaskUpdate,
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
		retryCtx, cancel = context.WithTimeout(ctx, c.controllerLeaderWaitDuration())
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
