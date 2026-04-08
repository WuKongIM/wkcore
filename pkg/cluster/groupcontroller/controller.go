package groupcontroller

import (
	"context"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
)

type Controller struct {
	store   *controllermeta.Store
	planner *Planner
	now     func() time.Time
}

func NewController(store *controllermeta.Store, cfg PlannerConfig) *Controller {
	return &Controller{
		store:   store,
		planner: NewPlanner(cfg),
		now:     time.Now,
	}
}

func (c *Controller) Tick(ctx context.Context) error {
	state, err := c.snapshot(ctx)
	if err != nil {
		return err
	}

	decision, err := c.planner.NextDecision(ctx, state)
	if err != nil {
		return err
	}
	if decision.GroupID == 0 {
		return nil
	}
	if decision.Assignment.GroupID != 0 {
		if err := c.store.UpsertAssignment(ctx, decision.Assignment); err != nil {
			return err
		}
	}
	if decision.Task == nil {
		return nil
	}
	task := *decision.Task
	if task.Status == controllermeta.TaskStatusUnknown {
		task.Status = controllermeta.TaskStatusPending
	}
	if task.NextRunAt.IsZero() {
		task.NextRunAt = state.Now
	}
	return c.store.UpsertTask(ctx, task)
}

func (c *Controller) snapshot(ctx context.Context) (PlannerState, error) {
	nodes, err := c.store.ListNodes(ctx)
	if err != nil {
		return PlannerState{}, err
	}
	assignments, err := c.store.ListAssignments(ctx)
	if err != nil {
		return PlannerState{}, err
	}
	views, err := c.store.ListRuntimeViews(ctx)
	if err != nil {
		return PlannerState{}, err
	}
	tasks, err := c.store.ListTasks(ctx)
	if err != nil {
		return PlannerState{}, err
	}

	state := PlannerState{
		Now:         c.now(),
		Nodes:       make(map[uint64]controllermeta.ClusterNode, len(nodes)),
		Assignments: make(map[uint32]controllermeta.GroupAssignment, len(assignments)),
		Runtime:     make(map[uint32]controllermeta.GroupRuntimeView, len(views)),
		Tasks:       make(map[uint32]controllermeta.ReconcileTask, len(tasks)),
	}
	for _, node := range nodes {
		state.Nodes[node.NodeID] = node
	}
	for _, assignment := range assignments {
		state.Assignments[assignment.GroupID] = assignment
	}
	for _, view := range views {
		state.Runtime[view.GroupID] = view
	}
	for _, task := range tasks {
		state.Tasks[task.GroupID] = task
	}
	return state, nil
}
