package multiisr_test

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/isr"
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
