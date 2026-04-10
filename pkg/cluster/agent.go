package cluster

import (
	"context"
	"errors"
	"sync"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
)

type assignmentTaskState struct {
	assignment controllermeta.SlotAssignment
	task       controllermeta.ReconcileTask
}

type slotAgent struct {
	cluster *Cluster
	client  controllerAPI
	cache   *assignmentCache
	reports pendingTaskReports
}

type pendingTaskReport struct {
	task    controllermeta.ReconcileTask
	taskErr error
}

type pendingTaskReports struct {
	mu sync.Mutex
	m  map[uint32]pendingTaskReport
}

func (a *slotAgent) HeartbeatOnce(ctx context.Context) error {
	if a == nil || a.cluster == nil || a.client == nil {
		return ErrNotStarted
	}
	now := time.Now()
	err := a.client.Report(ctx, slotcontrollerReport(a.cluster, now, nil))
	if err != nil {
		return err
	}

	for _, slotID := range a.cluster.runtime.Slots() {
		status, err := a.cluster.runtime.Status(slotID)
		if err != nil {
			continue
		}
		view := buildRuntimeView(now, slotID, status, a.cluster.observationPeersForGroup(slotID))
		err = a.client.Report(ctx, slotcontrollerReport(a.cluster, now, &view))
		if err != nil && !isControllerRedirect(err) {
			return err
		}
	}
	return nil
}

func (a *slotAgent) SyncAssignments(ctx context.Context) error {
	if a == nil || a.client == nil {
		return ErrNotStarted
	}
	var assignments []controllermeta.SlotAssignment
	err := a.retryControllerCall(ctx, func(attemptCtx context.Context) error {
		var err error
		assignments, err = a.client.RefreshAssignments(attemptCtx)
		return err
	})
	if err == nil {
		if a.cache != nil {
			a.cache.SetAssignments(assignments)
		}
		return nil
	}
	if !controllerReadFallbackAllowed(err) || a.cluster == nil || a.cluster.controllerMeta == nil {
		return err
	}
	assignments, err = a.cluster.controllerMeta.ListAssignments(ctx)
	if err != nil {
		return err
	}
	if a.cache != nil {
		a.cache.SetAssignments(assignments)
	}
	return nil
}

func (a *slotAgent) ApplyAssignments(ctx context.Context) error {
	if a == nil || a.cluster == nil || a.client == nil || a.cache == nil {
		return ErrNotStarted
	}

	assignments := a.cache.Snapshot()
	if len(assignments) == 0 {
		return nil
	}
	now := time.Now()
	desiredLocalSlots := make(map[uint32]struct{}, len(assignments))
	nodeByID := make(map[uint64]controllermeta.ClusterNode)
	if nodes, err := a.listControllerNodes(ctx); err == nil {
		nodeByID = make(map[uint64]controllermeta.ClusterNode, len(nodes))
		for _, node := range nodes {
			nodeByID[node.NodeID] = node
		}
	}

	viewByGroup := make(map[uint32]controllermeta.SlotRuntimeView, len(assignments))
	if views, err := a.listRuntimeViews(ctx); err == nil {
		for _, view := range views {
			viewByGroup[view.SlotID] = view
		}
	}

	for _, assignment := range assignments {
		if !assignmentContainsPeer(assignment.DesiredPeers, uint64(a.cluster.cfg.NodeID)) {
			continue
		}
		desiredLocalSlots[assignment.SlotID] = struct{}{}
		_, hasView := viewByGroup[assignment.SlotID]
		if err := a.cluster.ensureManagedSlotLocal(
			ctx,
			multiraft.SlotID(assignment.SlotID),
			assignment.DesiredPeers,
			hasView,
			false,
		); err != nil {
			return err
		}
	}

	taskByGroup := make(map[uint32]controllermeta.ReconcileTask, len(assignments))
	for _, assignment := range assignments {
		if pending, ok := a.pendingTaskReport(assignment.SlotID); ok {
			taskByGroup[assignment.SlotID] = pending.task
			continue
		}
		task, err := a.getTask(ctx, assignment.SlotID)
		if errors.Is(err, controllermeta.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		taskByGroup[assignment.SlotID] = task
	}

	protectedSourceSlots := make(map[uint32]struct{}, len(taskByGroup))
	for _, assignment := range assignments {
		_, hasView := viewByGroup[assignment.SlotID]
		task, ok := taskByGroup[assignment.SlotID]
		if ok && taskKeepsSourceGroupOpen(task, uint64(a.cluster.cfg.NodeID)) {
			protectedSourceSlots[assignment.SlotID] = struct{}{}
			if err := a.cluster.ensureManagedSlotLocal(
				ctx,
				multiraft.SlotID(assignment.SlotID),
				assignment.DesiredPeers,
				hasView,
				false,
			); err != nil {
				return err
			}
		}
	}
	for _, slotID := range a.cluster.runtime.Slots() {
		if _, ok := desiredLocalSlots[uint32(slotID)]; ok {
			continue
		}
		if _, ok := protectedSourceSlots[uint32(slotID)]; ok {
			continue
		}
		if err := a.cluster.runtime.CloseSlot(ctx, slotID); err != nil && !errors.Is(err, multiraft.ErrSlotNotFound) {
			return err
		}
		a.cluster.deleteRuntimePeers(slotID)
	}

	for _, assignment := range assignments {
		task, ok := taskByGroup[assignment.SlotID]
		if !ok {
			a.clearPendingTaskReport(assignment.SlotID)
			continue
		}
		_, hasView := viewByGroup[assignment.SlotID]
		bootstrapAuthorized := task.Kind == controllermeta.TaskKindBootstrap &&
			reconcileTaskRunnable(now, task)
		if bootstrapAuthorized {
			if err := a.cluster.ensureManagedSlotLocal(
				ctx,
				multiraft.SlotID(assignment.SlotID),
				assignment.DesiredPeers,
				hasView,
				true,
			); err != nil {
				return err
			}
		}
		localAssigned := assignmentContainsPeer(assignment.DesiredPeers, uint64(a.cluster.cfg.NodeID))
		localSourceProtected := taskKeepsSourceGroupOpen(task, uint64(a.cluster.cfg.NodeID))
		if !localAssigned && !localSourceProtected {
			a.clearPendingTaskReport(assignment.SlotID)
			continue
		}
		if pending, ok := a.pendingTaskReport(assignment.SlotID); ok {
			if !sameReconcileTaskIdentity(pending.task, task) {
				a.clearPendingTaskReport(assignment.SlotID)
			} else {
				reportErr := a.reportTaskResult(ctx, pending.task, pending.taskErr)
				if reportErr != nil {
					return reportErr
				}
				a.clearPendingTaskReport(assignment.SlotID)
				continue
			}
		}
		runnable := reconcileTaskRunnable(now, task)
		shouldExecute := a.shouldExecuteTask(assignment, task, nodeByID)
		if !runnable || !shouldExecute {
			continue
		}
		freshTask, err := a.getTask(ctx, assignment.SlotID)
		switch {
		case errors.Is(err, controllermeta.ErrNotFound):
			continue
		case err != nil:
			if !controllerReadFallbackAllowed(err) {
				return err
			}
			// The earlier task snapshot already proved this attempt was runnable.
			// If a best-effort revalidation times out during controller churn, keep
			// the known task moving rather than starving the retry loop.
			freshTask = task
		case !sameReconcileTaskIdentity(freshTask, task):
			continue
		}
		task = freshTask
		execErr := a.cluster.executeReconcileTask(ctx, assignmentTaskState{
			assignment: assignment,
			task:       task,
		})
		reportErr := a.reportTaskResult(ctx, task, execErr)
		if reportErr != nil {
			a.storePendingTaskReport(assignment.SlotID, task, execErr)
			return reportErr
		}
		a.clearPendingTaskReport(assignment.SlotID)
	}
	return nil
}

func (a *slotAgent) shouldExecuteTask(assignment controllermeta.SlotAssignment, task controllermeta.ReconcileTask, nodes map[uint64]controllermeta.ClusterNode) bool {
	if len(assignment.DesiredPeers) == 0 {
		return false
	}
	localNodeID := uint64(a.cluster.cfg.NodeID)
	switch task.Kind {
	case controllermeta.TaskKindRepair, controllermeta.TaskKindRebalance:
		if task.SourceNode == localNodeID {
			return true
		}
		if shouldPreferSourceTaskExecutor(task, nodes) {
			return false
		}
		leaderID, err := a.cluster.currentManagedSlotLeader(multiraft.SlotID(assignment.SlotID))
		if err == nil {
			return uint64(leaderID) == localNodeID
		}
	}

	minPeer := uint64(0)
	for _, peer := range assignment.DesiredPeers {
		node, ok := nodes[peer]
		if ok && node.Status != controllermeta.NodeStatusAlive {
			continue
		}
		if minPeer == 0 || peer < minPeer {
			minPeer = peer
		}
	}
	if minPeer == 0 {
		minPeer = assignment.DesiredPeers[0]
		for _, peer := range assignment.DesiredPeers[1:] {
			if peer < minPeer {
				minPeer = peer
			}
		}
	}
	return minPeer == localNodeID
}

func shouldPreferSourceTaskExecutor(task controllermeta.ReconcileTask, nodes map[uint64]controllermeta.ClusterNode) bool {
	if task.SourceNode == 0 || len(nodes) == 0 {
		return false
	}
	node, ok := nodes[task.SourceNode]
	if !ok {
		return false
	}
	switch node.Status {
	case controllermeta.NodeStatusAlive, controllermeta.NodeStatusDraining:
		return true
	default:
		return false
	}
}

func taskKeepsSourceGroupOpen(task controllermeta.ReconcileTask, localNodeID uint64) bool {
	if task.SourceNode == 0 || task.SourceNode != localNodeID {
		return false
	}
	switch task.Kind {
	case controllermeta.TaskKindRepair, controllermeta.TaskKindRebalance:
		return true
	default:
		return false
	}
}

func (a *slotAgent) reportTaskResult(ctx context.Context, task controllermeta.ReconcileTask, taskErr error) error {
	if a == nil || a.cluster == nil || a.client == nil {
		return ErrNotStarted
	}
	return a.cluster.retryControllerCommand(ctx, func(attemptCtx context.Context) error {
		return a.client.ReportTaskResult(attemptCtx, task, taskErr)
	})
}

func (a *slotAgent) listControllerNodes(ctx context.Context) ([]controllermeta.ClusterNode, error) {
	if a == nil || a.client == nil {
		return nil, ErrNotStarted
	}
	var nodes []controllermeta.ClusterNode
	err := a.retryControllerCall(ctx, func(attemptCtx context.Context) error {
		var err error
		nodes, err = a.client.ListNodes(attemptCtx)
		return err
	})
	if err == nil || !controllerReadFallbackAllowed(err) || a.cluster == nil || a.cluster.controllerMeta == nil {
		return nodes, err
	}
	return a.cluster.controllerMeta.ListNodes(ctx)
}

func (a *slotAgent) listRuntimeViews(ctx context.Context) ([]controllermeta.SlotRuntimeView, error) {
	if a == nil || a.client == nil {
		return nil, ErrNotStarted
	}
	var views []controllermeta.SlotRuntimeView
	err := a.retryControllerCall(ctx, func(attemptCtx context.Context) error {
		var err error
		views, err = a.client.ListRuntimeViews(attemptCtx)
		return err
	})
	if err == nil || !controllerReadFallbackAllowed(err) || a.cluster == nil || a.cluster.controllerMeta == nil {
		return views, err
	}
	return a.cluster.controllerMeta.ListRuntimeViews(ctx)
}

func (a *slotAgent) getTask(ctx context.Context, slotID uint32) (controllermeta.ReconcileTask, error) {
	if a == nil || a.client == nil {
		return controllermeta.ReconcileTask{}, ErrNotStarted
	}
	var task controllermeta.ReconcileTask
	err := a.retryControllerCall(ctx, func(attemptCtx context.Context) error {
		var err error
		task, err = a.client.GetTask(attemptCtx, slotID)
		return err
	})
	if err == nil || !controllerReadFallbackAllowed(err) || a.cluster == nil || a.cluster.controllerMeta == nil {
		return task, err
	}
	return a.cluster.controllerMeta.GetTask(ctx, slotID)
}

func (a *slotAgent) retryControllerCall(ctx context.Context, fn func(context.Context) error) error {
	if a == nil || a.client == nil {
		return ErrNotStarted
	}
	if a.cluster != nil {
		return a.cluster.retryControllerCommand(ctx, fn)
	}
	return fn(ctx)
}

func (a *slotAgent) pendingTaskReport(slotID uint32) (pendingTaskReport, bool) {
	if a == nil {
		return pendingTaskReport{}, false
	}
	a.reports.mu.Lock()
	defer a.reports.mu.Unlock()
	if a.reports.m == nil {
		return pendingTaskReport{}, false
	}
	report, ok := a.reports.m[slotID]
	return report, ok
}

func (a *slotAgent) storePendingTaskReport(slotID uint32, task controllermeta.ReconcileTask, taskErr error) {
	if a == nil {
		return
	}
	a.reports.mu.Lock()
	defer a.reports.mu.Unlock()
	if a.reports.m == nil {
		a.reports.m = make(map[uint32]pendingTaskReport)
	}
	a.reports.m[slotID] = pendingTaskReport{task: task, taskErr: taskErr}
}

func (a *slotAgent) clearPendingTaskReport(slotID uint32) {
	if a == nil {
		return
	}
	a.reports.mu.Lock()
	defer a.reports.mu.Unlock()
	if a.reports.m == nil {
		return
	}
	delete(a.reports.m, slotID)
}

func sameReconcileTaskIdentity(left, right controllermeta.ReconcileTask) bool {
	return left.SlotID == right.SlotID &&
		left.Kind == right.Kind &&
		left.Step == right.Step &&
		left.SourceNode == right.SourceNode &&
		left.TargetNode == right.TargetNode &&
		left.Attempt == right.Attempt
}

func slotcontrollerReport(c *Cluster, now time.Time, view *controllermeta.SlotRuntimeView) slotcontroller.AgentReport {
	return slotcontroller.AgentReport{
		NodeID:         uint64(c.cfg.NodeID),
		Addr:           c.controllerReportAddr(),
		ObservedAt:     now,
		CapacityWeight: 1,
		Runtime:        view,
	}
}
