package cluster

import (
	"reflect"
	"testing"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
)

func TestObservationCacheUpsertNodeReportKeepsNewestObservation(t *testing.T) {
	cache := newObservationCache()
	older := slotcontroller.AgentReport{
		NodeID:               1,
		Addr:                 "old",
		ObservedAt:           time.Unix(10, 0),
		CapacityWeight:       1,
		HashSlotTableVersion: 2,
	}
	newer := slotcontroller.AgentReport{
		NodeID:               1,
		Addr:                 "new",
		ObservedAt:           time.Unix(20, 0),
		CapacityWeight:       3,
		HashSlotTableVersion: 5,
	}

	cache.applyNodeReport(older)
	cache.applyNodeReport(newer)
	cache.applyNodeReport(older)

	snapshot := cache.snapshot()
	if len(snapshot.Nodes) != 1 {
		t.Fatalf("snapshot.Nodes len = %d, want 1", len(snapshot.Nodes))
	}
	got := snapshot.Nodes[0]
	if got.NodeID != newer.NodeID {
		t.Fatalf("snapshot.Nodes[0].NodeID = %d, want %d", got.NodeID, newer.NodeID)
	}
	if got.Addr != newer.Addr {
		t.Fatalf("snapshot.Nodes[0].Addr = %q, want %q", got.Addr, newer.Addr)
	}
	if !got.ObservedAt.Equal(newer.ObservedAt) {
		t.Fatalf("snapshot.Nodes[0].ObservedAt = %v, want %v", got.ObservedAt, newer.ObservedAt)
	}
	if got.CapacityWeight != newer.CapacityWeight {
		t.Fatalf("snapshot.Nodes[0].CapacityWeight = %d, want %d", got.CapacityWeight, newer.CapacityWeight)
	}
	if got.HashSlotTableVersion != newer.HashSlotTableVersion {
		t.Fatalf("snapshot.Nodes[0].HashSlotTableVersion = %d, want %d", got.HashSlotTableVersion, newer.HashSlotTableVersion)
	}
}

func TestObservationCacheUpsertRuntimeViewDeduplicatesUnchangedView(t *testing.T) {
	cache := newObservationCache()
	older := controllermeta.SlotRuntimeView{
		SlotID:              7,
		CurrentPeers:        []uint64{1, 2, 3},
		LeaderID:            2,
		HealthyVoters:       3,
		HasQuorum:           true,
		ObservedConfigEpoch: 9,
		LastReportAt:        time.Unix(10, 0),
	}
	newer := controllermeta.SlotRuntimeView{
		SlotID:              7,
		CurrentPeers:        []uint64{1, 2, 3},
		LeaderID:            2,
		HealthyVoters:       3,
		HasQuorum:           true,
		ObservedConfigEpoch: 9,
		LastReportAt:        time.Unix(20, 0),
	}

	cache.applyRuntimeView(older)
	firstPeerPtr := &cache.runtimeViews[older.SlotID].CurrentPeers[0]
	cache.applyRuntimeView(newer)

	stored := cache.runtimeViews[older.SlotID]
	if !stored.LastReportAt.Equal(newer.LastReportAt) {
		t.Fatalf("runtimeViews[%d].LastReportAt = %v, want %v", older.SlotID, stored.LastReportAt, newer.LastReportAt)
	}
	if &stored.CurrentPeers[0] != firstPeerPtr {
		t.Fatal("applyRuntimeView() replaced unchanged CurrentPeers slice, want deduplicated reuse")
	}
}

func TestObservationCacheSnapshotReturnsStableCopies(t *testing.T) {
	cache := newObservationCache()
	cache.applyNodeReport(slotcontroller.AgentReport{
		NodeID:               2,
		Addr:                 "node-2",
		ObservedAt:           time.Unix(30, 0),
		CapacityWeight:       4,
		HashSlotTableVersion: 11,
	})
	cache.applyRuntimeView(controllermeta.SlotRuntimeView{
		SlotID:              9,
		CurrentPeers:        []uint64{2, 3, 4},
		LeaderID:            3,
		HealthyVoters:       3,
		HasQuorum:           true,
		ObservedConfigEpoch: 12,
		LastReportAt:        time.Unix(40, 0),
	})

	snapshot := cache.snapshot()
	snapshot.Nodes[0].Addr = "mutated"
	snapshot.RuntimeViews[0].CurrentPeers[0] = 99

	next := cache.snapshot()
	if got, want := next.Nodes[0].Addr, "node-2"; got != want {
		t.Fatalf("snapshot node addr = %q, want %q", got, want)
	}
	if got, want := next.RuntimeViews[0].CurrentPeers, []uint64{2, 3, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot runtime peers = %v, want %v", got, want)
	}
}
