package raftcluster

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/groupcontroller"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
)

type assignmentTaskState struct {
	assignment controllermeta.GroupAssignment
	task       controllermeta.ReconcileTask
}

type groupAgent struct {
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

func (a *groupAgent) HeartbeatOnce(ctx context.Context) error {
	if a == nil || a.cluster == nil || a.client == nil {
		return ErrNotStarted
	}
	now := time.Now()
	baseCtx, cancel := withControllerTimeout(ctx)
	err := a.client.Report(baseCtx, groupcontrollerReport(a.cluster, now, nil))
	cancel()
	if err != nil {
		return err
	}

	for _, groupID := range a.cluster.runtime.Groups() {
		status, err := a.cluster.runtime.Status(groupID)
		if err != nil {
			continue
		}
		view := buildRuntimeView(now, groupID, status, a.cluster.observationPeersForGroup(groupID))
		reportCtx, cancel := withControllerTimeout(ctx)
		err = a.client.Report(reportCtx, groupcontrollerReport(a.cluster, now, &view))
		cancel()
		if err != nil && !isControllerRedirect(err) {
			return err
		}
	}
	return nil
}

func (a *groupAgent) SyncAssignments(ctx context.Context) error {
	if a == nil || a.client == nil {
		return ErrNotStarted
	}
	var assignments []controllermeta.GroupAssignment
	err := a.retryControllerCall(ctx, func(attemptCtx context.Context) error {
		queryCtx, cancel := withControllerTimeout(attemptCtx)
		defer cancel()
		var err error
		assignments, err = a.client.RefreshAssignments(queryCtx)
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

func (a *groupAgent) ApplyAssignments(ctx context.Context) error {
	if a == nil || a.cluster == nil || a.client == nil || a.cache == nil {
		return ErrNotStarted
	}

	assignments := a.cache.Snapshot()
	if len(assignments) == 0 {
		return nil
	}
	now := time.Now()
	desiredLocalGroups := make(map[uint32]struct{}, len(assignments))
	nodeByID := make(map[uint64]controllermeta.ClusterNode)
	if nodes, err := a.listControllerNodes(ctx); err == nil {
		nodeByID = make(map[uint64]controllermeta.ClusterNode, len(nodes))
		for _, node := range nodes {
			nodeByID[node.NodeID] = node
		}
	}

	viewByGroup := make(map[uint32]controllermeta.GroupRuntimeView, len(assignments))
	if views, err := a.listRuntimeViews(ctx); err == nil {
		for _, view := range views {
			viewByGroup[view.GroupID] = view
		}
	}

	for _, assignment := range assignments {
		if !assignmentContainsPeer(assignment.DesiredPeers, uint64(a.cluster.cfg.NodeID)) {
			continue
		}
		desiredLocalGroups[assignment.GroupID] = struct{}{}
		_, hasView := viewByGroup[assignment.GroupID]
		if err := a.cluster.ensureManagedGroupLocal(
			ctx,
			multiraft.GroupID(assignment.GroupID),
			assignment.DesiredPeers,
			hasView,
			false,
		); err != nil {
			return err
		}
	}

	taskByGroup := make(map[uint32]controllermeta.ReconcileTask, len(assignments))
	for _, assignment := range assignments {
		task, err := a.getTask(ctx, assignment.GroupID)
		if errors.Is(err, controllermeta.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		taskByGroup[assignment.GroupID] = task
	}

	protectedSourceGroups := make(map[uint32]struct{}, len(taskByGroup))
	for _, assignment := range assignments {
		_, hasView := viewByGroup[assignment.GroupID]
		task, ok := taskByGroup[assignment.GroupID]
		if ok && taskKeepsSourceGroupOpen(task, uint64(a.cluster.cfg.NodeID)) {
			protectedSourceGroups[assignment.GroupID] = struct{}{}
			if err := a.cluster.ensureManagedGroupLocal(
				ctx,
				multiraft.GroupID(assignment.GroupID),
				assignment.DesiredPeers,
				hasView,
				false,
			); err != nil {
				return err
			}
		}
	}
	for _, groupID := range a.cluster.runtime.Groups() {
		if _, ok := desiredLocalGroups[uint32(groupID)]; ok {
			continue
		}
		if _, ok := protectedSourceGroups[uint32(groupID)]; ok {
			continue
		}
		if err := a.cluster.runtime.CloseGroup(ctx, groupID); err != nil && !errors.Is(err, multiraft.ErrGroupNotFound) {
			return err
		}
		a.cluster.deleteRuntimePeers(groupID)
	}

	for _, assignment := range assignments {
		task, ok := taskByGroup[assignment.GroupID]
		if !ok {
			a.clearPendingTaskReport(assignment.GroupID)
			continue
		}
		_, hasView := viewByGroup[assignment.GroupID]
		bootstrapAuthorized := task.Kind == controllermeta.TaskKindBootstrap &&
			reconcileTaskRunnable(now, task)
		if bootstrapAuthorized {
			if err := a.cluster.ensureManagedGroupLocal(
				ctx,
				multiraft.GroupID(assignment.GroupID),
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
			a.clearPendingTaskReport(assignment.GroupID)
			continue
		}
		if pending, ok := a.pendingTaskReport(assignment.GroupID); ok {
			if !sameReconcileTaskIdentity(pending.task, task) {
				a.clearPendingTaskReport(assignment.GroupID)
			} else {
				reportErr := a.reportTaskResult(ctx, assignment.GroupID, pending.taskErr)
				if reportErr != nil {
					return reportErr
				}
				a.clearPendingTaskReport(assignment.GroupID)
				continue
			}
		}
		runnable := reconcileTaskRunnable(now, task)
		shouldExecute := a.shouldExecuteTask(assignment, task, nodeByID)
		if !runnable || !shouldExecute {
			continue
		}
		freshTask, err := a.getTask(ctx, assignment.GroupID)
		if errors.Is(err, controllermeta.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if !sameReconcileTaskIdentity(freshTask, task) {
			continue
		}
		task = freshTask
		execErr := a.cluster.executeReconcileTask(ctx, assignmentTaskState{
			assignment: assignment,
			task:       task,
		})
		reportErr := a.reportTaskResult(ctx, assignment.GroupID, execErr)
		if reportErr != nil {
			a.storePendingTaskReport(assignment.GroupID, task, execErr)
			return reportErr
		}
		a.clearPendingTaskReport(assignment.GroupID)
	}
	return nil
}

func (a *groupAgent) shouldExecuteTask(assignment controllermeta.GroupAssignment, task controllermeta.ReconcileTask, nodes map[uint64]controllermeta.ClusterNode) bool {
	if len(assignment.DesiredPeers) == 0 {
		return false
	}
	localNodeID := uint64(a.cluster.cfg.NodeID)
	switch task.Kind {
	case controllermeta.TaskKindRepair, controllermeta.TaskKindRebalance:
		if shouldPreferSourceTaskExecutor(task, nodes) {
			return task.SourceNode == localNodeID
		}
		leaderID, err := a.cluster.currentManagedGroupLeader(multiraft.GroupID(assignment.GroupID))
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

func (a *groupAgent) reportTaskResult(ctx context.Context, groupID uint32, taskErr error) error {
	if a == nil || a.cluster == nil || a.client == nil {
		return ErrNotStarted
	}
	return a.cluster.retryControllerCommand(ctx, func(attemptCtx context.Context) error {
		reportCtx, cancel := withControllerTimeout(attemptCtx)
		defer cancel()
		return a.client.ReportTaskResult(reportCtx, groupID, taskErr)
	})
}

func (a *groupAgent) listControllerNodes(ctx context.Context) ([]controllermeta.ClusterNode, error) {
	if a == nil || a.client == nil {
		return nil, ErrNotStarted
	}
	var nodes []controllermeta.ClusterNode
	err := a.retryControllerCall(ctx, func(attemptCtx context.Context) error {
		queryCtx, cancel := withControllerTimeout(attemptCtx)
		defer cancel()
		var err error
		nodes, err = a.client.ListNodes(queryCtx)
		return err
	})
	if err == nil || !controllerReadFallbackAllowed(err) || a.cluster == nil || a.cluster.controllerMeta == nil {
		return nodes, err
	}
	return a.cluster.controllerMeta.ListNodes(ctx)
}

func (a *groupAgent) listRuntimeViews(ctx context.Context) ([]controllermeta.GroupRuntimeView, error) {
	if a == nil || a.client == nil {
		return nil, ErrNotStarted
	}
	var views []controllermeta.GroupRuntimeView
	err := a.retryControllerCall(ctx, func(attemptCtx context.Context) error {
		queryCtx, cancel := withControllerTimeout(attemptCtx)
		defer cancel()
		var err error
		views, err = a.client.ListRuntimeViews(queryCtx)
		return err
	})
	if err == nil || !controllerReadFallbackAllowed(err) || a.cluster == nil || a.cluster.controllerMeta == nil {
		return views, err
	}
	return a.cluster.controllerMeta.ListRuntimeViews(ctx)
}

func (a *groupAgent) getTask(ctx context.Context, groupID uint32) (controllermeta.ReconcileTask, error) {
	if a == nil || a.client == nil {
		return controllermeta.ReconcileTask{}, ErrNotStarted
	}
	var task controllermeta.ReconcileTask
	err := a.retryControllerCall(ctx, func(attemptCtx context.Context) error {
		queryCtx, cancel := withControllerTimeout(attemptCtx)
		defer cancel()
		var err error
		task, err = a.client.GetTask(queryCtx, groupID)
		return err
	})
	if err == nil || !controllerReadFallbackAllowed(err) || a.cluster == nil || a.cluster.controllerMeta == nil {
		return task, err
	}
	return a.cluster.controllerMeta.GetTask(ctx, groupID)
}

func (a *groupAgent) retryControllerCall(ctx context.Context, fn func(context.Context) error) error {
	if a == nil || a.client == nil {
		return ErrNotStarted
	}
	if a.cluster != nil {
		return a.cluster.retryControllerCommand(ctx, fn)
	}
	return fn(ctx)
}

func (a *groupAgent) pendingTaskReport(groupID uint32) (pendingTaskReport, bool) {
	if a == nil {
		return pendingTaskReport{}, false
	}
	a.reports.mu.Lock()
	defer a.reports.mu.Unlock()
	if a.reports.m == nil {
		return pendingTaskReport{}, false
	}
	report, ok := a.reports.m[groupID]
	return report, ok
}

func (a *groupAgent) storePendingTaskReport(groupID uint32, task controllermeta.ReconcileTask, taskErr error) {
	if a == nil {
		return
	}
	a.reports.mu.Lock()
	defer a.reports.mu.Unlock()
	if a.reports.m == nil {
		a.reports.m = make(map[uint32]pendingTaskReport)
	}
	a.reports.m[groupID] = pendingTaskReport{task: task, taskErr: taskErr}
}

func (a *groupAgent) clearPendingTaskReport(groupID uint32) {
	if a == nil {
		return
	}
	a.reports.mu.Lock()
	defer a.reports.mu.Unlock()
	if a.reports.m == nil {
		return
	}
	delete(a.reports.m, groupID)
}

func sameReconcileTaskIdentity(left, right controllermeta.ReconcileTask) bool {
	return left.GroupID == right.GroupID &&
		left.Kind == right.Kind &&
		left.Step == right.Step &&
		left.SourceNode == right.SourceNode &&
		left.TargetNode == right.TargetNode &&
		left.Attempt == right.Attempt
}

func groupcontrollerReport(c *Cluster, now time.Time, view *controllermeta.GroupRuntimeView) groupcontroller.AgentReport {
	return groupcontroller.AgentReport{
		NodeID:         uint64(c.cfg.NodeID),
		Addr:           c.controllerReportAddr(),
		ObservedAt:     now,
		CapacityWeight: 1,
		Runtime:        view,
	}
}
