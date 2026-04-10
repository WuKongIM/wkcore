package isr

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestApplyMetaRejectsInvalidISRSubset(t *testing.T) {
	r := newTestReplica(t)

	err := r.ApplyMeta(ChannelMeta{
		ChannelKey: "group-10",
		Epoch:      1,
		Leader:     1,
		Replicas:   []NodeID{1, 2},
		ISR:        []NodeID{1, 3},
		MinISR:     2,
	})
	if !errors.Is(err, ErrInvalidMeta) {
		t.Fatalf("expected ErrInvalidMeta, got %v", err)
	}
}

func TestChannelMetaRejectsEmptyChannelKey(t *testing.T) {
	r := newTestReplica(t)

	err := r.ApplyMeta(ChannelMeta{
		ChannelKey: "",
		Epoch:      1,
		Leader:     1,
		Replicas:   []NodeID{1, 2},
		ISR:        []NodeID{1, 2},
		MinISR:     1,
	})
	if !errors.Is(err, ErrInvalidMeta) {
		t.Fatalf("expected ErrInvalidMeta, got %v", err)
	}
}

func TestApplyMetaNormalizesReplicaAndISRLists(t *testing.T) {
	r := newTestReplica(t)

	if err := r.ApplyMeta(ChannelMeta{
		ChannelKey: "group-10",
		Epoch:      3,
		Leader:     2,
		Replicas:   []NodeID{3, 2, 3, 1, 2},
		ISR:        []NodeID{2, 1, 2},
		MinISR:     2,
	}); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}

	if got := r.meta.Replicas; !reflect.DeepEqual(got, []NodeID{3, 2, 1}) {
		t.Fatalf("Replicas = %v", got)
	}
	if got := r.meta.ISR; !reflect.DeepEqual(got, []NodeID{2, 1}) {
		t.Fatalf("ISR = %v", got)
	}
}

func TestApplyMetaRejectsSameEpochLeaderChange(t *testing.T) {
	r := newTestReplica(t)
	if err := r.ApplyMeta(ChannelMeta{
		ChannelKey: "group-10",
		Epoch:      3,
		Leader:     1,
		Replicas:   []NodeID{1, 2},
		ISR:        []NodeID{1, 2},
		MinISR:     2,
	}); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}

	err := r.ApplyMeta(ChannelMeta{
		ChannelKey: "group-10",
		Epoch:      3,
		Leader:     2,
		Replicas:   []NodeID{1, 2},
		ISR:        []NodeID{1, 2},
		MinISR:     2,
	})
	if !errors.Is(err, ErrStaleMeta) {
		t.Fatalf("expected ErrStaleMeta, got %v", err)
	}
}

func TestBecomeFollowerAppliesMetaAndRole(t *testing.T) {
	r := newTestReplica(t)

	err := r.BecomeFollower(ChannelMeta{
		ChannelKey: "group-10",
		Epoch:      4,
		Leader:     2,
		Replicas:   []NodeID{1, 2},
		ISR:        []NodeID{1, 2},
		MinISR:     2,
	})
	if err != nil {
		t.Fatalf("BecomeFollower() error = %v", err)
	}

	st := r.Status()
	if st.Role != RoleFollower {
		t.Fatalf("Role = %v", st.Role)
	}
	if st.Leader != 2 || st.Epoch != 4 || st.ChannelKey != "group-10" {
		t.Fatalf("status = %+v", st)
	}
}

func TestTombstoneFencesFutureOperations(t *testing.T) {
	r := newTestReplica(t)
	if err := r.Tombstone(); err != nil {
		t.Fatalf("Tombstone() error = %v", err)
	}

	_, err := r.Append(context.Background(), []Record{{Payload: []byte("x"), SizeBytes: 1}})
	if !errors.Is(err, ErrTombstoned) {
		t.Fatalf("expected ErrTombstoned, got %v", err)
	}
}
