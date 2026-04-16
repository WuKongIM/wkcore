package cluster

import (
	"context"
	"errors"
	"sync"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
)

type healthTimer interface {
	Stop() bool
}

type nodeHealthSchedulerConfig struct {
	suspectTimeout time.Duration
	deadTimeout    time.Duration
	now            func() time.Time
	afterFunc      func(time.Duration, func()) healthTimer
	loadNode       func(context.Context, uint64) (controllermeta.ClusterNode, error)
	propose        func(context.Context, slotcontroller.Command) error
}

type nodeHealthScheduler struct {
	cfg nodeHealthSchedulerConfig

	mu    sync.Mutex
	nodes map[uint64]*nodeHealthState
}

type nodeHealthState struct {
	observation   nodeObservation
	generation    uint64
	suspectAt     time.Time
	deadAt        time.Time
	suspectTimer  healthTimer
	deadTimer     healthTimer
	pendingStatus *controllermeta.NodeStatus
}

func newNodeHealthScheduler(cfg nodeHealthSchedulerConfig) *nodeHealthScheduler {
	if cfg.now == nil {
		cfg.now = time.Now
	}
	if cfg.afterFunc == nil {
		cfg.afterFunc = func(delay time.Duration, fn func()) healthTimer {
			if delay < 0 {
				delay = 0
			}
			return time.AfterFunc(delay, fn)
		}
	}
	return &nodeHealthScheduler{
		cfg:   cfg,
		nodes: make(map[uint64]*nodeHealthState),
	}
}

func (s *nodeHealthScheduler) observe(observation nodeObservation) {
	if s == nil || observation.NodeID == 0 {
		return
	}

	s.mu.Lock()
	state := s.ensureNodeLocked(observation.NodeID)
	if !state.observation.ObservedAt.IsZero() && observation.ObservedAt.Before(state.observation.ObservedAt) {
		s.mu.Unlock()
		return
	}

	state.observation = observation
	state.generation++
	state.suspectAt = observation.ObservedAt.Add(s.cfg.suspectTimeout)
	state.deadAt = observation.ObservedAt.Add(s.cfg.deadTimeout)
	if state.suspectTimer != nil {
		state.suspectTimer.Stop()
	}
	if state.deadTimer != nil {
		state.deadTimer.Stop()
	}
	generation := state.generation
	if s.cfg.suspectTimeout > 0 {
		suspectAt := state.suspectAt
		delay := suspectAt.Sub(s.cfg.now())
		state.suspectTimer = s.cfg.afterFunc(delay, func() {
			s.handleDeadline(observation.NodeID, generation, controllermeta.NodeStatusSuspect, suspectAt)
		})
	}
	if s.cfg.deadTimeout > 0 {
		deadAt := state.deadAt
		delay := deadAt.Sub(s.cfg.now())
		state.deadTimer = s.cfg.afterFunc(delay, func() {
			s.handleDeadline(observation.NodeID, generation, controllermeta.NodeStatusDead, deadAt)
		})
	}
	s.mu.Unlock()

	s.proposeStatusTransition(observation, controllermeta.NodeStatusAlive, observation.ObservedAt)
}

func (s *nodeHealthScheduler) handleDeadline(nodeID uint64, generation uint64, targetStatus controllermeta.NodeStatus, evaluatedAt time.Time) {
	if s == nil || nodeID == 0 {
		return
	}

	s.mu.Lock()
	state, ok := s.nodes[nodeID]
	if !ok || state.generation != generation {
		s.mu.Unlock()
		return
	}
	switch targetStatus {
	case controllermeta.NodeStatusSuspect:
		if !state.suspectAt.Equal(evaluatedAt) {
			s.mu.Unlock()
			return
		}
	case controllermeta.NodeStatusDead:
		if !state.deadAt.Equal(evaluatedAt) {
			s.mu.Unlock()
			return
		}
	default:
		s.mu.Unlock()
		return
	}
	observation := state.observation
	s.mu.Unlock()

	s.proposeStatusTransition(observation, targetStatus, evaluatedAt)
}

func (s *nodeHealthScheduler) reset() {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, state := range s.nodes {
		if state.suspectTimer != nil {
			state.suspectTimer.Stop()
		}
		if state.deadTimer != nil {
			state.deadTimer.Stop()
		}
	}
	s.nodes = make(map[uint64]*nodeHealthState)
}

func (s *nodeHealthScheduler) handleCommittedCommand(slotcontroller.Command) {
}

func (s *nodeHealthScheduler) ensureNodeLocked(nodeID uint64) *nodeHealthState {
	state, ok := s.nodes[nodeID]
	if ok {
		return state
	}
	state = &nodeHealthState{}
	s.nodes[nodeID] = state
	return state
}

func (s *nodeHealthScheduler) proposeStatusTransition(observation nodeObservation, desired controllermeta.NodeStatus, evaluatedAt time.Time) {
	if s == nil || s.cfg.propose == nil || s.cfg.loadNode == nil {
		return
	}

	node, err := s.cfg.loadNode(context.Background(), observation.NodeID)
	if err != nil && !errors.Is(err, controllermeta.ErrNotFound) {
		return
	}

	transition, ok := buildNodeStatusTransition(observation, node, err, desired, evaluatedAt)
	if !ok {
		return
	}

	if !s.beginPending(observation.NodeID, desired) {
		return
	}
	defer s.clearPending(observation.NodeID, desired)

	_ = s.cfg.propose(context.Background(), slotcontroller.Command{
		Kind: slotcontroller.CommandKindNodeStatusUpdate,
		NodeStatusUpdate: &slotcontroller.NodeStatusUpdate{
			Transitions: []slotcontroller.NodeStatusTransition{transition},
		},
	})
}

func (s *nodeHealthScheduler) beginPending(nodeID uint64, desired controllermeta.NodeStatus) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.ensureNodeLocked(nodeID)
	if state.pendingStatus != nil && *state.pendingStatus == desired {
		return false
	}
	next := desired
	state.pendingStatus = &next
	return true
}

func (s *nodeHealthScheduler) clearPending(nodeID uint64, desired controllermeta.NodeStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.nodes[nodeID]
	if !ok || state.pendingStatus == nil || *state.pendingStatus != desired {
		return
	}
	state.pendingStatus = nil
}

func buildNodeStatusTransition(observation nodeObservation, node controllermeta.ClusterNode, loadErr error, desired controllermeta.NodeStatus, evaluatedAt time.Time) (slotcontroller.NodeStatusTransition, bool) {
	switch {
	case errors.Is(loadErr, controllermeta.ErrNotFound):
		if desired != controllermeta.NodeStatusAlive || observation.Addr == "" {
			return slotcontroller.NodeStatusTransition{}, false
		}
		return slotcontroller.NodeStatusTransition{
			NodeID:         observation.NodeID,
			NewStatus:      desired,
			EvaluatedAt:    evaluatedAt,
			Addr:           observation.Addr,
			CapacityWeight: normalizeCapacityWeight(observation.CapacityWeight),
		}, true
	case loadErr != nil:
		return slotcontroller.NodeStatusTransition{}, false
	}

	if node.Status == controllermeta.NodeStatusDraining && desired != controllermeta.NodeStatusDraining {
		return slotcontroller.NodeStatusTransition{}, false
	}
	if node.Status == desired {
		return slotcontroller.NodeStatusTransition{}, false
	}
	if desired == controllermeta.NodeStatusSuspect && node.Status != controllermeta.NodeStatusAlive {
		return slotcontroller.NodeStatusTransition{}, false
	}
	if desired == controllermeta.NodeStatusDead && node.Status != controllermeta.NodeStatusAlive && node.Status != controllermeta.NodeStatusSuspect {
		return slotcontroller.NodeStatusTransition{}, false
	}
	if desired == controllermeta.NodeStatusAlive && node.Status == controllermeta.NodeStatusAlive {
		return slotcontroller.NodeStatusTransition{}, false
	}

	expected := node.Status
	return slotcontroller.NodeStatusTransition{
		NodeID:         observation.NodeID,
		NewStatus:      desired,
		ExpectedStatus: &expected,
		EvaluatedAt:    evaluatedAt,
		Addr:           observation.Addr,
		CapacityWeight: normalizeCapacityWeight(observation.CapacityWeight),
	}, true
}

func normalizeCapacityWeight(weight int) int {
	if weight <= 0 {
		return 1
	}
	return weight
}
