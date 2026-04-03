package multiraft

import (
	"context"
	"testing"
	"time"
)

func TestThreeNodeClusterElectsLeaderAndReplicatesOverAsyncNetwork(t *testing.T) {
	cluster := newAsyncTestCluster(t, []NodeID{1, 2, 3}, asyncNetworkConfig{
		MaxDelay: 5 * time.Millisecond,
		Seed:     1,
	})
	groupID := GroupID(200)

	cluster.bootstrapGroup(t, groupID, []NodeID{1, 2, 3})
	cluster.waitForBootstrapApplied(t, groupID, 3)

	leaderID := cluster.waitForLeader(t, groupID)
	fut, err := cluster.runtime(leaderID).Propose(context.Background(), groupID, []byte("set e2e=1"))
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}

	res := waitForFutureResult(t, fut)
	if string(res.Data) != "ok:set e2e=1" {
		t.Fatalf("Wait().Data = %q", res.Data)
	}

	cluster.waitForAllApplied(t, groupID, []byte("set e2e=1"))
}
