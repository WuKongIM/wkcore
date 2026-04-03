package isrnode_test

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isrnode"
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
	if got := env.generations.stored[31]; got != 2 {
		t.Fatalf("expected generation 2 after re-ensure, got %d", got)
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
