package multiisr

import (
	"slices"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
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

func TestSchedulerPendingQueuePreservesFIFO(t *testing.T) {
	var q schedulerQueue

	for _, groupID := range []uint64{11, 12, 13} {
		q.enqueue(groupID)
	}

	for _, want := range []uint64{11, 12, 13} {
		got, ok := q.pop()
		if !ok {
			t.Fatalf("pop() missing group %d", want)
		}
		if got != want {
			t.Fatalf("pop() = %d, want %d", got, want)
		}
	}

	if _, ok := q.pop(); ok {
		t.Fatal("expected empty pending queue after drain")
	}
}

func TestSchedulerPendingQueueReusableAfterDrain(t *testing.T) {
	var q schedulerQueue

	q.enqueue(21)
	q.enqueue(22)
	if _, ok := q.pop(); !ok {
		t.Fatal("expected first queued group")
	}
	if _, ok := q.pop(); !ok {
		t.Fatal("expected second queued group")
	}

	q.enqueue(31)
	q.enqueue(32)

	for _, want := range []uint64{31, 32} {
		got, ok := q.pop()
		if !ok {
			t.Fatalf("pop() missing group %d after reuse", want)
		}
		if got != want {
			t.Fatalf("pop() = %d, want %d", got, want)
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
