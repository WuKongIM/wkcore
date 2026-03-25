package multiraft

import (
	"context"
	"errors"
	"testing"
)

func TestChangeConfigAppliesAddLearner(t *testing.T) {
	rt := newStartedRuntime(t)
	groupID := openSingleNodeLeader(t, rt, 30)

	fut, err := rt.ChangeConfig(context.Background(), groupID, ConfigChange{
		Type:   AddLearner,
		NodeID: 2,
	})
	if err != nil {
		t.Fatalf("ChangeConfig() error = %v", err)
	}
	if _, err := fut.Wait(context.Background()); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
}

func TestTransferLeadershipRejectsUnknownGroup(t *testing.T) {
	rt := newStartedRuntime(t)
	err := rt.TransferLeadership(context.Background(), 999, 2)
	if !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("expected ErrGroupNotFound, got %v", err)
	}
}
