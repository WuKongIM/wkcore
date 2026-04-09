package groupcontroller

import (
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
)

type NodeStatus = controllermeta.NodeStatus
type TaskStatus = controllermeta.TaskStatus

const (
	NodeStatusAlive    = controllermeta.NodeStatusAlive
	NodeStatusSuspect  = controllermeta.NodeStatusSuspect
	NodeStatusDead     = controllermeta.NodeStatusDead
	NodeStatusDraining = controllermeta.NodeStatusDraining

	TaskStatusPending  = controllermeta.TaskStatusPending
	TaskStatusRetrying = controllermeta.TaskStatusRetrying
	TaskStatusFailed   = controllermeta.TaskStatusFailed
)

type PlannerConfig struct {
	GroupCount             uint32
	ReplicaN               int
	RebalanceSkewThreshold int
	MaxTaskAttempts        int
	RetryBackoffBase       time.Duration
}

type PlannerState struct {
	Now         time.Time
	Nodes       map[uint64]controllermeta.ClusterNode
	Assignments map[uint32]controllermeta.GroupAssignment
	Runtime     map[uint32]controllermeta.GroupRuntimeView
	Tasks       map[uint32]controllermeta.ReconcileTask
}

type Decision struct {
	GroupID    uint32
	Assignment controllermeta.GroupAssignment
	Task       *controllermeta.ReconcileTask
	Degraded   bool
}
