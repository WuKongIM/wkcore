package groupcontroller

import (
	"context"
	"sort"

	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
)

type Planner struct {
	cfg PlannerConfig
}

func NewPlanner(cfg PlannerConfig) *Planner {
	if cfg.RebalanceSkewThreshold <= 0 {
		cfg.RebalanceSkewThreshold = 1
	}
	return &Planner{cfg: cfg}
}

func (p *Planner) ReconcileGroup(_ context.Context, state PlannerState, groupID uint32) (Decision, error) {
	decision := Decision{GroupID: groupID}

	assignment, hasAssignment := state.Assignments[groupID]
	view, hasView := state.Runtime[groupID]
	if task, ok := state.Tasks[groupID]; ok && task.Status != controllermeta.TaskStatusFailed {
		decision.Assignment = assignment
		decision.Task = &task
		return decision, nil
	}
	if !hasAssignment && !hasView {
		peers := p.selectBootstrapPeers(state)
		if len(peers) < p.cfg.ReplicaN {
			return decision, nil
		}
		decision.Assignment = controllermeta.GroupAssignment{
			GroupID:      groupID,
			DesiredPeers: peers,
		}
		decision.Task = &controllermeta.ReconcileTask{
			GroupID:    groupID,
			Kind:       controllermeta.TaskKindBootstrap,
			Step:       controllermeta.TaskStepAddLearner,
			TargetNode: peers[0],
		}
		return decision, nil
	}
	if !hasAssignment {
		return decision, nil
	}
	if hasView && !view.HasQuorum {
		decision.Degraded = true
		decision.Assignment = assignment
		return decision, nil
	}

	deadOrDraining := p.firstPeerNeedingRepair(state, assignment.DesiredPeers)
	if deadOrDraining == 0 {
		decision.Assignment = assignment
		return decision, nil
	}

	target := p.selectRepairTarget(state, assignment.DesiredPeers)
	if target == 0 {
		decision.Assignment = assignment
		return decision, nil
	}

	desiredPeers := replacePeer(assignment.DesiredPeers, deadOrDraining, target)
	decision.Assignment = controllermeta.GroupAssignment{
		GroupID:        assignment.GroupID,
		DesiredPeers:   desiredPeers,
		ConfigEpoch:    assignment.ConfigEpoch,
		BalanceVersion: assignment.BalanceVersion,
	}
	decision.Task = &controllermeta.ReconcileTask{
		GroupID:    groupID,
		Kind:       controllermeta.TaskKindRepair,
		Step:       controllermeta.TaskStepAddLearner,
		SourceNode: deadOrDraining,
		TargetNode: target,
	}
	return decision, nil
}

func (p *Planner) NextDecision(ctx context.Context, state PlannerState) (Decision, error) {
	for groupID := uint32(1); groupID <= p.cfg.GroupCount; groupID++ {
		decision, err := p.ReconcileGroup(ctx, state, groupID)
		if err != nil {
			return Decision{}, err
		}
		if decision.Degraded || decision.Task != nil {
			return decision, nil
		}
	}

	return p.nextRebalanceDecision(state), nil
}

func (p *Planner) nextRebalanceDecision(state PlannerState) Decision {
	loads := groupLoads(state.Assignments)
	minNode, minLoad, maxNode, maxLoad := loadExtremes(state, loads)
	if minNode == 0 || maxNode == 0 || maxLoad-minLoad < p.cfg.RebalanceSkewThreshold {
		return Decision{}
	}

	candidates := make([]controllermeta.GroupAssignment, 0, len(state.Assignments))
	for _, assignment := range state.Assignments {
		if containsPeer(assignment.DesiredPeers, maxNode) && !containsPeer(assignment.DesiredPeers, minNode) {
			if _, ok := state.Runtime[assignment.GroupID]; !ok {
				continue
			}
			candidates = append(candidates, assignment)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].BalanceVersion == candidates[j].BalanceVersion {
			return candidates[i].GroupID < candidates[j].GroupID
		}
		return candidates[i].BalanceVersion < candidates[j].BalanceVersion
	})
	for _, assignment := range candidates {
		if task, ok := state.Tasks[assignment.GroupID]; ok && task.Status != controllermeta.TaskStatusFailed {
			continue
		}
		if p.firstPeerNeedingRepair(state, assignment.DesiredPeers) != 0 {
			continue
		}
		decision := Decision{
			GroupID: assignment.GroupID,
			Assignment: controllermeta.GroupAssignment{
				GroupID:        assignment.GroupID,
				DesiredPeers:   replacePeer(assignment.DesiredPeers, maxNode, minNode),
				ConfigEpoch:    assignment.ConfigEpoch,
				BalanceVersion: assignment.BalanceVersion + 1,
			},
		}
		decision.Task = &controllermeta.ReconcileTask{
			GroupID:    assignment.GroupID,
			Kind:       controllermeta.TaskKindRebalance,
			Step:       controllermeta.TaskStepAddLearner,
			SourceNode: maxNode,
			TargetNode: minNode,
		}
		return decision
	}
	return Decision{}
}

func (p *Planner) selectBootstrapPeers(state PlannerState) []uint64 {
	candidates := make([]uint64, 0, len(state.Nodes))
	for nodeID, node := range state.Nodes {
		if node.Status != controllermeta.NodeStatusAlive {
			continue
		}
		candidates = append(candidates, nodeID)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i] < candidates[j] })
	if len(candidates) > p.cfg.ReplicaN {
		candidates = candidates[:p.cfg.ReplicaN]
	}
	return candidates
}

func (p *Planner) firstPeerNeedingRepair(state PlannerState, peers []uint64) uint64 {
	for _, peer := range peers {
		node, ok := state.Nodes[peer]
		if !ok || node.Status == controllermeta.NodeStatusDead || node.Status == controllermeta.NodeStatusDraining {
			return peer
		}
	}
	return 0
}

func (p *Planner) selectRepairTarget(state PlannerState, peers []uint64) uint64 {
	loads := groupLoads(state.Assignments)
	type candidate struct {
		nodeID uint64
		load   int
	}
	candidates := make([]candidate, 0, len(state.Nodes))
	for nodeID, node := range state.Nodes {
		if node.Status != controllermeta.NodeStatusAlive {
			continue
		}
		if containsPeer(peers, nodeID) {
			continue
		}
		candidates = append(candidates, candidate{nodeID: nodeID, load: loads[nodeID]})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].load == candidates[j].load {
			return candidates[i].nodeID < candidates[j].nodeID
		}
		return candidates[i].load < candidates[j].load
	})
	if len(candidates) == 0 {
		return 0
	}
	return candidates[0].nodeID
}

func groupLoads(assignments map[uint32]controllermeta.GroupAssignment) map[uint64]int {
	loads := make(map[uint64]int)
	for _, assignment := range assignments {
		for _, peer := range assignment.DesiredPeers {
			loads[peer]++
		}
	}
	return loads
}

func loadExtremes(state PlannerState, loads map[uint64]int) (minNode uint64, minLoad int, maxNode uint64, maxLoad int) {
	first := true
	for nodeID, node := range state.Nodes {
		if node.Status != controllermeta.NodeStatusAlive {
			continue
		}
		load := loads[nodeID]
		if first {
			minNode, minLoad = nodeID, load
			maxNode, maxLoad = nodeID, load
			first = false
			continue
		}
		if load < minLoad || (load == minLoad && nodeID < minNode) {
			minNode, minLoad = nodeID, load
		}
		if load > maxLoad || (load == maxLoad && nodeID < maxNode) {
			maxNode, maxLoad = nodeID, load
		}
	}
	return minNode, minLoad, maxNode, maxLoad
}

func containsPeer(peers []uint64, nodeID uint64) bool {
	for _, peer := range peers {
		if peer == nodeID {
			return true
		}
	}
	return false
}

func replacePeer(peers []uint64, source, target uint64) []uint64 {
	next := make([]uint64, 0, len(peers))
	for _, peer := range peers {
		if peer == source {
			continue
		}
		next = append(next, peer)
	}
	next = append(next, target)
	sort.Slice(next, func(i, j int) bool { return next[i] < next[j] })
	return next
}
