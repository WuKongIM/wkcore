package multiisr

import (
	"slices"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/isr"
)

func TestSchedulerRunsTasksInRequiredOrder(t *testing.T) {
	g, log := newScheduledTestGroup()
	g.markSnapshot()
	g.markLease()
	g.markCommit()
	g.markReplication()
	g.markControl()

	runGroupOnce(g)
	want := []string{"control", "replication", "commit", "lease", "snapshot"}
	if !slices.Equal(want, *log) {
		t.Fatalf("task order mismatch: want %v, got %v", want, *log)
	}
}

func TestGroupPopReplicationPeerPreservesFIFO(t *testing.T) {
	g := &group{}
	for _, peer := range []uint64{2, 3, 4} {
		g.enqueueReplication(isr.NodeID(peer))
	}

	for _, want := range []isr.NodeID{2, 3, 4} {
		got, ok := g.popReplicationPeer()
		if !ok {
			t.Fatalf("popReplicationPeer() missing peer %d", want)
		}
		if got != want {
			t.Fatalf("popReplicationPeer() = %d, want %d", got, want)
		}
	}

	if _, ok := g.popReplicationPeer(); ok {
		t.Fatal("expected empty replication peer queue after drain")
	}
}

func TestGroupPopReplicationPeerQueueReusableAfterDrain(t *testing.T) {
	g := &group{}
	for _, peer := range []uint64{2, 3} {
		g.enqueueReplication(isr.NodeID(peer))
	}

	for range 2 {
		if _, ok := g.popReplicationPeer(); !ok {
			t.Fatal("expected queued peer before drain")
		}
	}

	g.enqueueReplication(5)
	g.enqueueReplication(6)

	for _, want := range []isr.NodeID{5, 6} {
		got, ok := g.popReplicationPeer()
		if !ok {
			t.Fatalf("popReplicationPeer() missing peer %d after reuse", want)
		}
		if got != want {
			t.Fatalf("popReplicationPeer() = %d, want %d", got, want)
		}
	}
}

func newScheduledTestGroup() (*group, *[]string) {
	log := make([]string, 0, 5)
	g := &group{
		onControl: func() {
			log = append(log, "control")
		},
		onReplication: func() {
			log = append(log, "replication")
		},
		onCommit: func() {
			log = append(log, "commit")
		},
		onLease: func() {
			log = append(log, "lease")
		},
		onSnapshot: func() {
			log = append(log, "snapshot")
		},
	}
	return g, &log
}

func runGroupOnce(g *group) {
	g.runPendingTasks()
}
