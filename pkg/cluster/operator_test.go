package cluster

import (
	"context"
	"testing"
	"time"
)

const testControllerLeaderWaitTimeout = 25 * time.Millisecond

func TestRetryControllerCommandUsesClusterLeaderWaitTimeoutOverride(t *testing.T) {
	cluster := &Cluster{
		controllerResources: controllerResources{
			controllerLeaderWaitTimeout: testControllerLeaderWaitTimeout,
		},
	}

	var observedRemaining time.Duration
	err := cluster.retryControllerCommand(context.Background(), func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("retryControllerCommand() context has no deadline")
		}
		observedRemaining = time.Until(deadline)
		return nil
	})
	if err != nil {
		t.Fatalf("retryControllerCommand() error = %v", err)
	}
	if observedRemaining > 200*time.Millisecond {
		t.Fatalf("retryControllerCommand() deadline remaining = %v, want <= %v", observedRemaining, 200*time.Millisecond)
	}
}

func TestRetryControllerCommandUsesScaledRetryInterval(t *testing.T) {
	cluster := &Cluster{
		controllerResources: controllerResources{
			controllerLeaderWaitTimeout: 25 * time.Millisecond,
		},
	}

	attempts := 0
	err := cluster.retryControllerCommand(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 2 {
			return ErrNotLeader
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retryControllerCommand() error = %v", err)
	}
	if attempts != 2 {
		t.Fatalf("retryControllerCommand() attempts = %d, want 2", attempts)
	}
}
