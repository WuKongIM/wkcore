package multiisr

import (
	"slices"
	"testing"
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
