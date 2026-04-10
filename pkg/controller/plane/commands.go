package plane

import (
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
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
	Runtime        *controllermeta.SlotRuntimeView
}

type OperatorRequest struct {
	Kind   OperatorKind
	NodeID uint64
}

type TaskAdvance struct {
	SlotID  uint32
	Attempt uint32
	Now     time.Time
	Err     error
}

type Command struct {
	Kind       CommandKind
	Report     *AgentReport
	Op         *OperatorRequest
	Advance    *TaskAdvance
	Assignment *controllermeta.SlotAssignment
	Task       *controllermeta.ReconcileTask
}
