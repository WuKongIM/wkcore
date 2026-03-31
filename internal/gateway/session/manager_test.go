package session

import "testing"

func newTestSession(id uint64) Session {
	return newSession(
		id,
		"listener-a",
		"remote-a",
		"local-a",
		1,
		1,
		nil,
	)
}

func TestManagerAddGetRemove(t *testing.T) {
	mgr := NewManager()
	sess := newTestSession(42)

	mgr.Add(sess)

	got, ok := mgr.Get(42)
	if !ok {
		t.Fatal("expected session to be present after Add")
	}
	if got.ID() != sess.ID() {
		t.Fatalf("expected session ID %d, got %d", sess.ID(), got.ID())
	}

	if count := mgr.Count(); count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}

	mgr.Remove(42)

	if _, ok := mgr.Get(42); ok {
		t.Fatal("expected session to be removed")
	}
	if count := mgr.Count(); count != 0 {
		t.Fatalf("expected count 0, got %d", count)
	}
}

func TestSessionCloseIsIdempotent(t *testing.T) {
	sess := newTestSession(7)

	if err := sess.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}
	if err := sess.Close(); err != nil {
		t.Fatalf("second close failed: %v", err)
	}
}
