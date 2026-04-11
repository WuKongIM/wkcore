package replica

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

func TestNewReplicaValidatesRequiredDependencies(t *testing.T) {
	_, err := NewReplica(ReplicaConfig{})
	if !errors.Is(err, channel.ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestReplicaInterfaceSurfaceCompiles(t *testing.T) {
	var r Replica = &replica{}
	_, _ = r.Append(context.Background(), nil)
	_ = r.Status()
}
