package isr

import (
	"context"
	"errors"
	"testing"
)

func TestNewReplicaValidatesRequiredDependencies(t *testing.T) {
	_, err := NewReplica(ReplicaConfig{})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestReplicaSurfaceUsesChannelKey(t *testing.T) {
	var req FetchRequest
	req.ChannelKey = ChannelKey("channel/1/YzE")
	req.ReplicaID = 2
	req.OffsetEpoch = 5

	var apply ApplyFetchRequest
	apply.ChannelKey = req.ChannelKey
	apply.Leader = 1

	var r Replica = &replica{}
	_, _ = r.Fetch(context.Background(), req)
	_ = r.ApplyFetch(context.Background(), apply)
}
