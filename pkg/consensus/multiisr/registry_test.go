package multiisr_test

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/consensus/multiisr"
	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

func TestEnsureGroupStoresNextGenerationBeforeReplicaCreation(t *testing.T) {
	env := newTestEnv(t)
	meta := testMeta(7, 1, 1, []isr.NodeID{1, 2})

	if err := env.runtime.EnsureGroup(meta); err != nil {
		t.Fatalf("EnsureGroup() error = %v", err)
	}
	if got := env.generations.stored[meta.GroupID]; got != 1 {
		t.Fatalf("expected generation 1, got %d", got)
	}
	if env.factory.created[0].generation != 1 {
		t.Fatalf("replica created with wrong generation")
	}
}

func TestRemoveGroupLeavesTombstoneThatDropsLateEnvelope(t *testing.T) {
	env := newTestEnv(t)
	meta := testMeta(9, 1, 1, []isr.NodeID{1, 2})
	mustEnsure(t, env.runtime, meta)
	mustRemove(t, env.runtime, meta.GroupID)

	env.transport.deliver(multiisr.Envelope{GroupID: 9, Generation: 1, Epoch: 1, Kind: multiisr.MessageKindAck})
	if env.factory.replicas[0].applyFetchCalls != 0 {
		t.Fatalf("late envelope should be dropped")
	}
}
