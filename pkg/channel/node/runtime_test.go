package node_test

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
	isrnode "github.com/WuKongIM/WuKongIM/pkg/channel/node"
)

func TestAppendChecksLeaseSynchronouslyBeforeReplicaAppend(t *testing.T) {
	env := newTestEnv(t)
	meta := fencedLeaderMeta(11)
	mustEnsure(t, env.runtime, meta)

	_, err := mustGroup(t, env.runtime, 11).Append(context.Background(), []isr.Record{{Payload: []byte("x"), SizeBytes: 1}})
	if !errors.Is(err, isr.ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}
	if env.factory.replicas[0].appendCalls != 0 {
		t.Fatalf("append should be fenced before reaching replica")
	}
}

func TestRuntimeReconcileFlowEnsureApplyRemoveEnsure(t *testing.T) {
	env := newTestEnv(t)
	meta1 := testMeta(31, 1, 1, []isr.NodeID{1, 2})
	meta2 := testMeta(31, 2, 2, []isr.NodeID{1, 2})

	mustEnsure(t, env.runtime, meta1)
	if err := env.runtime.ApplyMeta(meta2); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	mustRemove(t, env.runtime, 31)
	if err := env.runtime.EnsureGroup(meta2); err != nil {
		t.Fatalf("EnsureGroup() after remove error = %v", err)
	}
	if got := env.generations.stored[testGroupKey(31)]; got != 2 {
		t.Fatalf("expected generation 2 after re-ensure, got %d", got)
	}
}

func TestEnsureGroupPromotesLocalLeaderReplica(t *testing.T) {
	env := newTestEnv(t)

	mustEnsure(t, env.runtime, testMeta(32, 1, 1, []isr.NodeID{1, 2}))

	if got := mustGroup(t, env.runtime, 32).Status().Role; got != isr.RoleLeader {
		t.Fatalf("expected RoleLeader, got %v", got)
	}
}

func TestApplyMetaTransitionsReplicaRoleWithLeadershipChanges(t *testing.T) {
	env := newTestEnv(t)

	mustEnsure(t, env.runtime, testMeta(33, 1, 2, []isr.NodeID{1, 2}))
	if got := mustGroup(t, env.runtime, 33).Status().Role; got != isr.RoleFollower {
		t.Fatalf("expected initial RoleFollower, got %v", got)
	}

	if err := env.runtime.ApplyMeta(testMeta(33, 2, 1, []isr.NodeID{1, 2})); err != nil {
		t.Fatalf("ApplyMeta() promote leader error = %v", err)
	}
	if got := mustGroup(t, env.runtime, 33).Status().Role; got != isr.RoleLeader {
		t.Fatalf("expected promoted RoleLeader, got %v", got)
	}

	if err := env.runtime.ApplyMeta(testMeta(33, 3, 2, []isr.NodeID{1, 2})); err != nil {
		t.Fatalf("ApplyMeta() demote follower error = %v", err)
	}
	if got := mustGroup(t, env.runtime, 33).Status().Role; got != isr.RoleFollower {
		t.Fatalf("expected demoted RoleFollower, got %v", got)
	}
}

func TestApplyMetaSkipsReplicaUpdateWhenMetaUnchangedForFollower(t *testing.T) {
	env := newTestEnv(t)
	meta := testMeta(34, 1, 2, []isr.NodeID{1, 2})

	mustEnsure(t, env.runtime, meta)
	replica := env.factory.replicas[0]
	before := len(replica.metaCalls)

	if err := env.runtime.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	if got := len(replica.metaCalls); got != before {
		t.Fatalf("meta call count = %d, want %d", got, before)
	}
}

func TestApplyMetaSkipsReplicaUpdateWhenMetaUnchangedForLeader(t *testing.T) {
	env := newTestEnv(t)
	meta := testMeta(35, 1, 1, []isr.NodeID{1, 2})

	mustEnsure(t, env.runtime, meta)
	replica := env.factory.replicas[0]
	before := len(replica.metaCalls)

	if err := env.runtime.ApplyMeta(meta); err != nil {
		t.Fatalf("ApplyMeta() error = %v", err)
	}
	if got := len(replica.metaCalls); got != before {
		t.Fatalf("meta call count = %d, want %d", got, before)
	}
}

func TestEnsureGroupReturnsErrTooManyGroups(t *testing.T) {
	env := newTestEnvWithOptions(t, withMaxGroups(1))
	mustEnsure(t, env.runtime, testMeta(41, 1, 1, []isr.NodeID{1, 2}))
	err := env.runtime.EnsureGroup(testMeta(42, 1, 1, []isr.NodeID{1, 2}))
	if !errors.Is(err, isrnode.ErrTooManyGroups) {
		t.Fatalf("expected ErrTooManyGroups, got %v", err)
	}
}

func TestEnsureGroupRejectsDuplicateActiveGroup(t *testing.T) {
	env := newTestEnv(t)
	meta := testMeta(43, 1, 1, []isr.NodeID{1, 2})

	mustEnsure(t, env.runtime, meta)
	err := env.runtime.EnsureGroup(meta)
	if !errors.Is(err, isrnode.ErrGroupExists) {
		t.Fatalf("expected ErrGroupExists, got %v", err)
	}
}

func TestServeFetchRejectsUnknownGeneration(t *testing.T) {
	env := newTestEnv(t)
	mustEnsure(t, env.runtime, testMeta(44, 1, 1, []isr.NodeID{1, 2}))

	_, err := env.runtime.(isrnode.FetchService).ServeFetch(context.Background(), isrnode.FetchRequestEnvelope{
		GroupKey:    testGroupKey(44),
		Epoch:       1,
		Generation:  99,
		ReplicaID:   2,
		FetchOffset: 0,
		MaxBytes:    1 << 20,
	})
	if !errors.Is(err, isrnode.ErrGenerationMismatch) {
		t.Fatalf("expected ErrGenerationMismatch, got %v", err)
	}
	if env.factory.replicas[0].fetchCalls != 0 {
		t.Fatalf("expected fetch to be rejected before replica.Fetch")
	}
}

func TestServeFetchRejectsUnknownGroup(t *testing.T) {
	env := newTestEnv(t)

	_, err := env.runtime.(isrnode.FetchService).ServeFetch(context.Background(), isrnode.FetchRequestEnvelope{
		GroupKey:    testGroupKey(45),
		Epoch:       1,
		Generation:  1,
		ReplicaID:   2,
		FetchOffset: 0,
		MaxBytes:    1 << 20,
	})
	if !errors.Is(err, isrnode.ErrGroupNotFound) {
		t.Fatalf("expected ErrGroupNotFound, got %v", err)
	}
}

func TestServeFetchRejectsStaleEpoch(t *testing.T) {
	env := newTestEnv(t)
	mustEnsure(t, env.runtime, testMeta(46, 3, 1, []isr.NodeID{1, 2}))

	_, err := env.runtime.(isrnode.FetchService).ServeFetch(context.Background(), isrnode.FetchRequestEnvelope{
		GroupKey:    testGroupKey(46),
		Epoch:       2,
		Generation:  1,
		ReplicaID:   2,
		FetchOffset: 0,
		MaxBytes:    1 << 20,
	})
	if !errors.Is(err, isr.ErrStaleMeta) {
		t.Fatalf("expected ErrStaleMeta, got %v", err)
	}
	if env.factory.replicas[0].fetchCalls != 0 {
		t.Fatalf("expected stale epoch to be rejected before replica.Fetch")
	}
}
