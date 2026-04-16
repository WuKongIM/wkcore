package cluster

import (
	"sort"
	"sync"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
)

type nodeObservation struct {
	NodeID               uint64
	Addr                 string
	ObservedAt           time.Time
	CapacityWeight       int
	HashSlotTableVersion uint64
}

type observationSnapshot struct {
	Nodes        []nodeObservation
	RuntimeViews []controllermeta.SlotRuntimeView
}

type observationCache struct {
	mu           sync.RWMutex
	nodes        map[uint64]nodeObservation
	runtimeViews map[uint32]controllermeta.SlotRuntimeView
}

func newObservationCache() *observationCache {
	return &observationCache{
		nodes:        make(map[uint64]nodeObservation),
		runtimeViews: make(map[uint32]controllermeta.SlotRuntimeView),
	}
}

func (c *observationCache) applyNodeReport(report slotcontroller.AgentReport) {
	if c == nil || report.NodeID == 0 {
		return
	}
	observation := nodeObservation{
		NodeID:               report.NodeID,
		Addr:                 report.Addr,
		ObservedAt:           report.ObservedAt,
		CapacityWeight:       report.CapacityWeight,
		HashSlotTableVersion: report.HashSlotTableVersion,
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if current, ok := c.nodes[report.NodeID]; ok && observation.ObservedAt.Before(current.ObservedAt) {
		return
	}
	c.nodes[report.NodeID] = observation
}

func (c *observationCache) applyRuntimeView(view controllermeta.SlotRuntimeView) {
	if c == nil || view.SlotID == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	current, ok := c.runtimeViews[view.SlotID]
	if ok && view.LastReportAt.Before(current.LastReportAt) {
		return
	}
	if ok && runtimeViewEquivalent(current, view) {
		current.LastReportAt = view.LastReportAt
		c.runtimeViews[view.SlotID] = current
		return
	}
	view.CurrentPeers = cloneUint64Slice(view.CurrentPeers)
	c.runtimeViews[view.SlotID] = view
}

func (c *observationCache) snapshot() observationSnapshot {
	if c == nil {
		return observationSnapshot{}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	snapshot := observationSnapshot{
		Nodes:        make([]nodeObservation, 0, len(c.nodes)),
		RuntimeViews: make([]controllermeta.SlotRuntimeView, 0, len(c.runtimeViews)),
	}
	for _, node := range c.nodes {
		snapshot.Nodes = append(snapshot.Nodes, node)
	}
	for _, view := range c.runtimeViews {
		view.CurrentPeers = cloneUint64Slice(view.CurrentPeers)
		snapshot.RuntimeViews = append(snapshot.RuntimeViews, view)
	}
	sort.Slice(snapshot.Nodes, func(i, j int) bool {
		return snapshot.Nodes[i].NodeID < snapshot.Nodes[j].NodeID
	})
	sort.Slice(snapshot.RuntimeViews, func(i, j int) bool {
		return snapshot.RuntimeViews[i].SlotID < snapshot.RuntimeViews[j].SlotID
	})
	return snapshot
}

func (c *observationCache) reset() {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.nodes = make(map[uint64]nodeObservation)
	c.runtimeViews = make(map[uint32]controllermeta.SlotRuntimeView)
}

func runtimeViewEquivalent(left, right controllermeta.SlotRuntimeView) bool {
	if left.SlotID != right.SlotID ||
		left.LeaderID != right.LeaderID ||
		left.HealthyVoters != right.HealthyVoters ||
		left.HasQuorum != right.HasQuorum ||
		left.ObservedConfigEpoch != right.ObservedConfigEpoch ||
		len(left.CurrentPeers) != len(right.CurrentPeers) {
		return false
	}
	for i := range left.CurrentPeers {
		if left.CurrentPeers[i] != right.CurrentPeers[i] {
			return false
		}
	}
	return true
}

func cloneUint64Slice(src []uint64) []uint64 {
	if len(src) == 0 {
		return nil
	}
	dst := make([]uint64, len(src))
	copy(dst, src)
	return dst
}
