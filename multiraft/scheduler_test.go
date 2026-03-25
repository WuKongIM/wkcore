package multiraft

import "testing"

func TestSchedulerRequeueAfterDirtyDoneDeliversGroup(t *testing.T) {
	s := newScheduler()
	groupID := GroupID(42)

	s.enqueue(groupID)
	select {
	case got := <-s.ch:
		if got != groupID {
			t.Fatalf("first dequeue = %d", got)
		}
	default:
		t.Fatal("group was not enqueued")
	}

	s.begin(groupID)
	s.enqueue(groupID)
	if !s.done(groupID) {
		t.Fatal("done() = false, want true after dirty enqueue")
	}

	s.requeue(groupID)

	select {
	case got := <-s.ch:
		if got != groupID {
			t.Fatalf("requeued group = %d", got)
		}
	default:
		t.Fatal("dirty group was not delivered after requeue")
	}
}
