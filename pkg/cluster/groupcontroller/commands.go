package groupcontroller

import (
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
)

type CommandKind uint8

const (
	CommandKindUnknown CommandKind = iota
	CommandKindNodeHeartbeat
	CommandKindOperatorRequest
	CommandKindEvaluateTimeouts
	CommandKindTaskResult
	CommandKindAssignmentTaskUpdate
)

type OperatorKind uint8

const (
	OperatorKindUnknown OperatorKind = iota
	OperatorMarkNodeDraining
	OperatorResumeNode
)

type AgentReport struct {
	NodeID         uint64
	Addr           string
	ObservedAt     time.Time
	CapacityWeight int
	Runtime        *controllermeta.GroupRuntimeView
}

type OperatorRequest struct {
	Kind   OperatorKind
	NodeID uint64
}

type TaskAdvance struct {
	GroupID uint32
	Attempt uint32
	Now     time.Time
	Err     error
}

type Command struct {
	Kind       CommandKind
	Report     *AgentReport
	Op         *OperatorRequest
	Advance    *TaskAdvance
	Assignment *controllermeta.GroupAssignment
	Task       *controllermeta.ReconcileTask
}
