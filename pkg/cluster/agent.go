package cluster

import (
	"context"
	"sync"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
)

type assignmentTaskState struct {
	assignment controllermeta.SlotAssignment
	task       controllermeta.ReconcileTask
}

type assignmentReconciler interface {
	Tick(context.Context) error
}

type slotAgent struct {
	cluster    *Cluster
	client     controllerAPI
	cache      *assignmentCache
	reports    pendingTaskReports
	reconciler assignmentReconciler
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
		view := buildRuntimeView(now, slotID, status, a.cluster.observationPeersForSlot(slotID))
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
	if err := a.cluster.syncRouterHashSlotTableFromStore(ctx); err != nil {
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
	reconciler := a.assignmentReconciler()
	if reconciler == nil {
		return ErrNotStarted
	}
	return reconciler.Tick(ctx)
}

func (a *slotAgent) assignmentReconciler() assignmentReconciler {
	if a == nil {
		return nil
	}
	if a.reconciler == nil {
		a.reconciler = newReconciler(a)
	}
	return a.reconciler
}

func (a *slotAgent) reportTaskResult(ctx context.Context, task controllermeta.ReconcileTask, taskErr error) error {
	if a == nil || a.cluster == nil || a.client == nil {
		return ErrNotStarted
	}
	err := a.cluster.retryControllerCommand(ctx, func(attemptCtx context.Context) error {
		if a.cluster.isLocalControllerLeader() {
			return a.cluster.proposeTaskResultOnLeader(attemptCtx, task, taskErr)
		}
		return a.client.ReportTaskResult(attemptCtx, task, taskErr)
	})
	return err
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

func (a *slotAgent) listRuntimeViews(ctx context.Context) ([]controllermeta.SlotRuntimeView, bool, error) {
	if a == nil || a.client == nil {
		return nil, false, ErrNotStarted
	}
	var views []controllermeta.SlotRuntimeView
	err := a.retryControllerCall(ctx, func(attemptCtx context.Context) error {
		var err error
		views, err = a.client.ListRuntimeViews(attemptCtx)
		return err
	})
	if err == nil || !controllerReadFallbackAllowed(err) || a.cluster == nil || a.cluster.controllerMeta == nil {
		return views, err == nil, err
	}
	views, err = a.cluster.controllerMeta.ListRuntimeViews(ctx)
	return views, false, err
}

func (a *slotAgent) getTask(ctx context.Context, slotID uint32) (controllermeta.ReconcileTask, error) {
	if a == nil || a.client == nil {
		return controllermeta.ReconcileTask{}, ErrNotStarted
	}
	if a.cluster != nil && a.cluster.controllerMeta != nil && a.cluster.isLocalControllerLeader() {
		return a.cluster.controllerMeta.GetTask(ctx, slotID)
	}
	var task controllermeta.ReconcileTask
	err := a.retryControllerCall(ctx, func(attemptCtx context.Context) error {
		var err error
		task, err = a.client.GetTask(attemptCtx, slotID)
		return err
	})
	if err == nil || !controllerReadFallbackAllowed(err) || a.cluster == nil || a.cluster.controllerMeta == nil || !a.cluster.isLocalControllerLeader() {
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
		NodeID:               uint64(c.cfg.NodeID),
		Addr:                 c.controllerReportAddr(),
		ObservedAt:           now,
		CapacityWeight:       1,
		HashSlotTableVersion: c.HashSlotTableVersion(),
		Runtime:              view,
	}
}
