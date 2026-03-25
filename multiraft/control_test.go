package multiraft

import (
	"context"
	"errors"
	"testing"
	"time"
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

func TestChangeConfigCorrelatesFutureByCommittedIndex(t *testing.T) {
	rt := newStartedRuntime(t)
	groupID := openSingleNodeLeader(t, rt, 31)

	fut, err := rt.ChangeConfig(context.Background(), groupID, ConfigChange{
		Type:   AddLearner,
		NodeID: 2,
	})
	if err != nil {
		t.Fatalf("ChangeConfig() error = %v", err)
	}

	res, err := fut.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if res.Index == 0 {
		t.Fatalf("Wait().Index = 0")
	}

	st, err := rt.Status(groupID)
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	if res.Index != st.AppliedIndex {
		t.Fatalf("Wait().Index = %d, want applied index %d", res.Index, st.AppliedIndex)
	}
}

func TestRemoteConfigChangeDoesNotResolveLocalFuture(t *testing.T) {
	cluster := newAsyncTestCluster(t, []NodeID{1, 2, 3}, asyncNetworkConfig{
		MaxDelay: 5 * time.Millisecond,
		Seed:     14,
	})
	groupID := GroupID(32)

	cluster.bootstrapGroup(t, groupID, []NodeID{1, 2, 3})
	cluster.waitForBootstrapApplied(t, groupID, 3)

	oldLeader := cluster.waitForLeader(t, groupID)
	cluster.partitionNode(oldLeader)

	stale, err := cluster.runtime(oldLeader).ChangeConfig(context.Background(), groupID, ConfigChange{
		Type:   AddLearner,
		NodeID: 4,
	})
	if err != nil {
		t.Fatalf("ChangeConfig(stale) error = %v", err)
	}

	newLeader := cluster.waitForLeaderAmong(t, groupID, cluster.otherNodes(oldLeader))
	fresh, err := cluster.runtime(newLeader).ChangeConfig(context.Background(), groupID, ConfigChange{
		Type:   AddLearner,
		NodeID: 5,
	})
	if err != nil {
		t.Fatalf("ChangeConfig(fresh) error = %v", err)
	}

	freshRes, err := fresh.Wait(context.Background())
	if err != nil {
		t.Fatalf("fresh Wait() error = %v", err)
	}

	cluster.healNode(oldLeader)
	cluster.waitForNodeCommitIndex(t, oldLeader, groupID, freshRes.Index)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	res, err := stale.Wait(ctx)
	if err == nil {
		t.Fatalf("stale config future resolved unexpectedly: result=%+v err=%v", res, err)
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrNotLeader) {
		t.Fatalf("stale config future error = %v", err)
	}
}
