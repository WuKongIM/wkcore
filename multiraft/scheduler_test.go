package multiraft

import (
	"testing"
	"time"
)

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

func TestSchedulerEnqueueBuffersWhenChannelIsFull(t *testing.T) {
	s := newScheduler()
	s.ch = make(chan GroupID, 1)

	s.enqueue(1)

	done := make(chan struct{})
	go func() {
		s.enqueue(2)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("enqueue() blocked while scheduler channel was full")
	}

	first := <-s.ch
	if first != 1 {
		t.Fatalf("first dequeue = %d", first)
	}

	s.begin(first)

	select {
	case got := <-s.ch:
		if got != 2 {
			t.Fatalf("second dequeue = %d", got)
		}
	case <-time.After(time.Second):
		t.Fatal("buffered group was not dispatched after begin() freed capacity")
	}
}
