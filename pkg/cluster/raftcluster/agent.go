package raftcluster

import (
	"context"
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/groupcontroller"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
)

type assignmentTaskState struct {
	assignment controllermeta.GroupAssignment
	task       controllermeta.ReconcileTask
	view       controllermeta.GroupRuntimeView
}

type groupAgent struct {
	cluster *Cluster
	client  *controllerClient
	cache   *assignmentCache
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
	_, err := a.client.RefreshAssignments(ctx)
	return err
}

func (a *groupAgent) ApplyAssignments(ctx context.Context) error {
	if a == nil || a.cluster == nil || a.client == nil || a.cache == nil {
		return ErrNotStarted
	}

	assignments := a.cache.Snapshot()
	if len(assignments) == 0 {
		return nil
	}

	viewByGroup := make(map[uint32]controllermeta.GroupRuntimeView, len(assignments))
	if views, err := a.client.ListRuntimeViews(ctx); err == nil {
		for _, view := range views {
			viewByGroup[view.GroupID] = view
		}
	}

	for _, assignment := range assignments {
		if !assignmentContainsPeer(assignment.DesiredPeers, uint64(a.cluster.cfg.NodeID)) {
			continue
		}
		_, hasView := viewByGroup[assignment.GroupID]
		if err := a.cluster.ensureManagedGroupLocal(ctx, multiraft.GroupID(assignment.GroupID), assignment.DesiredPeers, !hasView); err != nil {
			return err
		}
	}

	for _, assignment := range assignments {
		task, err := a.client.GetTask(ctx, assignment.GroupID)
		if errors.Is(err, controllermeta.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if !reconcileTaskRunnable(time.Now(), task) || !a.shouldExecuteTask(assignment) {
			continue
		}
		execErr := a.cluster.executeReconcileTask(ctx, assignmentTaskState{
			assignment: assignment,
			task:       task,
			view:       viewByGroup[assignment.GroupID],
		})
		reportCtx, cancel := withControllerTimeout(ctx)
		reportErr := a.client.ReportTaskResult(reportCtx, assignment.GroupID, execErr)
		cancel()
		if reportErr != nil {
			return reportErr
		}
	}
	return nil
}

func (a *groupAgent) shouldExecuteTask(assignment controllermeta.GroupAssignment) bool {
	if len(assignment.DesiredPeers) == 0 {
		return false
	}
	minPeer := assignment.DesiredPeers[0]
	for _, peer := range assignment.DesiredPeers[1:] {
		if peer < minPeer {
			minPeer = peer
		}
	}
	return minPeer == uint64(a.cluster.cfg.NodeID)
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
