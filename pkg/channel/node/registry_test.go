package node_test

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
	isrnode "github.com/WuKongIM/WuKongIM/pkg/channel/node"
)

func TestEnsureChannelStoresGenerationByChannelKey(t *testing.T) {
	env := newTestEnv(t)
	meta := testMeta(7, 1, 1, []isr.NodeID{1, 2})

	if err := env.runtime.EnsureChannel(meta); err != nil {
		t.Fatalf("EnsureChannel() error = %v", err)
	}
	if got := env.generations.stored[meta.ChannelKey]; got != 1 {
		t.Fatalf("expected generation 1, got %d", got)
	}
	if env.factory.created[0].generation != 1 {
		t.Fatalf("replica created with wrong generation")
	}
}

func TestHandleEnvelopeDropsLateGenerationForChannelKey(t *testing.T) {
	env := newTestEnv(t)
	meta := testMeta(9, 1, 1, []isr.NodeID{1, 2})
	mustEnsure(t, env.runtime, meta)
	mustRemove(t, env.runtime, 9)

	env.transport.deliver(isrnode.Envelope{ChannelKey: testChannelKey(9), Generation: 1, Epoch: 1, Kind: isrnode.MessageKindAck})
	if env.factory.replicas[0].applyFetchCalls != 0 {
		t.Fatalf("late envelope should be dropped")
	}
}
