package groupcontroller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
)

type StateMachineConfig struct {
	SuspectTimeout   time.Duration
	DeadTimeout      time.Duration
	MaxTaskAttempts  int
	RetryBackoffBase time.Duration
}

type StateMachine struct {
	store *controllermeta.Store
	cfg   StateMachineConfig
}

func NewStateMachine(store *controllermeta.Store, cfg StateMachineConfig) *StateMachine {
	if cfg.MaxTaskAttempts <= 0 {
		cfg.MaxTaskAttempts = 3
	}
	if cfg.RetryBackoffBase <= 0 {
		cfg.RetryBackoffBase = time.Second
	}
	return &StateMachine{store: store, cfg: cfg}
}

func (sm *StateMachine) Apply(ctx context.Context, cmd Command) error {
	if sm == nil || sm.store == nil {
		return controllermeta.ErrClosed
	}
	switch cmd.Kind {
	case CommandKindNodeHeartbeat:
		if cmd.Report == nil {
			return controllermeta.ErrInvalidArgument
		}
		return sm.applyNodeHeartbeat(ctx, *cmd.Report)
	case CommandKindOperatorRequest:
		if cmd.Op == nil {
			return controllermeta.ErrInvalidArgument
		}
		return sm.applyOperatorRequest(ctx, *cmd.Op)
	case CommandKindEvaluateTimeouts:
		if cmd.Advance == nil {
			return controllermeta.ErrInvalidArgument
		}
		return sm.applyTimeoutEvaluation(ctx, cmd.Advance.Now)
	case CommandKindTaskResult:
		if cmd.Advance == nil {
			return controllermeta.ErrInvalidArgument
		}
		return sm.applyTaskResult(ctx, *cmd.Advance)
	case CommandKindAssignmentTaskUpdate:
		return sm.applyAssignmentTaskUpdate(ctx, cmd.Assignment, cmd.Task)
	default:
		return controllermeta.ErrInvalidArgument
	}
}

func (sm *StateMachine) applyNodeHeartbeat(ctx context.Context, report AgentReport) error {
	if report.NodeID == 0 || report.Addr == "" {
		return controllermeta.ErrInvalidArgument
	}

	node, err := sm.store.GetNode(ctx, report.NodeID)
	switch {
	case errors.Is(err, controllermeta.ErrNotFound):
		node = controllermeta.ClusterNode{NodeID: report.NodeID, CapacityWeight: 1}
	case err != nil:
		return err
	}

	node.Addr = report.Addr
	node.LastHeartbeatAt = report.ObservedAt
	if report.CapacityWeight > 0 {
		node.CapacityWeight = report.CapacityWeight
	}
	if node.CapacityWeight <= 0 {
		node.CapacityWeight = 1
	}
	if node.Status != controllermeta.NodeStatusDraining {
		node.Status = controllermeta.NodeStatusAlive
	}
	if err := sm.store.UpsertNode(ctx, node); err != nil {
		return err
	}

	if report.Runtime != nil {
		return sm.store.UpsertRuntimeView(ctx, *report.Runtime)
	}
	return nil
}

func (sm *StateMachine) applyOperatorRequest(ctx context.Context, op OperatorRequest) error {
	if op.NodeID == 0 {
		return controllermeta.ErrInvalidArgument
	}

	node, err := sm.store.GetNode(ctx, op.NodeID)
	switch {
	case errors.Is(err, controllermeta.ErrNotFound):
		node = controllermeta.ClusterNode{
			NodeID:         op.NodeID,
			Addr:           placeholderNodeAddr(op.NodeID),
			CapacityWeight: 1,
		}
	case err != nil:
		return err
	}

	switch op.Kind {
	case OperatorMarkNodeDraining:
		node.Status = controllermeta.NodeStatusDraining
	case OperatorResumeNode:
		node.Status = controllermeta.NodeStatusAlive
	default:
		return controllermeta.ErrInvalidArgument
	}
	return sm.store.UpsertNode(ctx, node)
}

func (sm *StateMachine) applyTimeoutEvaluation(ctx context.Context, now time.Time) error {
	nodes, err := sm.store.ListNodes(ctx)
	if err != nil {
		return err
	}

	for _, node := range nodes {
		if node.Status == controllermeta.NodeStatusDraining {
			continue
		}
		elapsed := now.Sub(node.LastHeartbeatAt)
		switch {
		case sm.cfg.DeadTimeout > 0 && elapsed > sm.cfg.DeadTimeout:
			node.Status = controllermeta.NodeStatusDead
		case sm.cfg.SuspectTimeout > 0 && elapsed >= sm.cfg.SuspectTimeout:
			node.Status = controllermeta.NodeStatusSuspect
		default:
			node.Status = controllermeta.NodeStatusAlive
		}
		if err := sm.store.UpsertNode(ctx, node); err != nil {
			return err
		}
	}
	return nil
}

func (sm *StateMachine) applyTaskResult(ctx context.Context, advance TaskAdvance) error {
	if advance.GroupID == 0 {
		return controllermeta.ErrInvalidArgument
	}

	task, err := sm.store.GetTask(ctx, advance.GroupID)
	if err != nil {
		return err
	}
	if advance.Err == nil {
		return sm.store.DeleteTask(ctx, advance.GroupID)
	}

	task.Attempt++
	task.LastError = advance.Err.Error()
	if int(task.Attempt) >= sm.cfg.MaxTaskAttempts {
		task.Status = controllermeta.TaskStatusFailed
		task.NextRunAt = advance.Now
		return sm.store.UpsertTask(ctx, task)
	}

	task.Status = controllermeta.TaskStatusRetrying
	task.NextRunAt = advance.Now.Add(sm.retryDelay(task.Attempt))
	return sm.store.UpsertTask(ctx, task)
}

func (sm *StateMachine) applyAssignmentTaskUpdate(ctx context.Context, assignment *controllermeta.GroupAssignment, task *controllermeta.ReconcileTask) error {
	switch {
	case assignment != nil && task != nil:
		return sm.store.UpsertAssignmentTask(ctx, *assignment, *task)
	case assignment != nil:
		return sm.store.UpsertAssignment(ctx, *assignment)
	case task != nil:
		return sm.store.UpsertTask(ctx, *task)
	default:
		return controllermeta.ErrInvalidArgument
	}
}

func (sm *StateMachine) retryDelay(attempt uint32) time.Duration {
	if attempt == 0 {
		return sm.cfg.RetryBackoffBase
	}
	shift := attempt - 1
	if shift > 30 {
		shift = 30
	}
	factor := time.Duration(1 << shift)
	if sm.cfg.RetryBackoffBase > 0 && factor > 0 && sm.cfg.RetryBackoffBase > time.Duration(math.MaxInt64/int64(factor)) {
		return time.Duration(math.MaxInt64)
	}
	return sm.cfg.RetryBackoffBase * factor
}

func placeholderNodeAddr(nodeID uint64) string {
	return fmt.Sprintf("operator://node-%d", nodeID)
}
