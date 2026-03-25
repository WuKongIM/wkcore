package multiraft

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

func TestProposeWaitReturnsAfterLocalApply(t *testing.T) {
	rt := newStartedRuntime(t)
	groupID := openSingleNodeLeader(t, rt, 10)

	fut, err := rt.Propose(context.Background(), groupID, []byte("set a=1"))
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}

	res, err := fut.Wait(context.Background())
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if string(res.Data) != "ok:set a=1" {
		t.Fatalf("Wait().Data = %q", res.Data)
	}
}

func TestReadyPipelinePersistsBeforeApply(t *testing.T) {
	rt := newStartedRuntime(t)
	groupID := openSingleNodeLeader(t, rt, 11)

	_, err := rt.Propose(context.Background(), groupID, []byte("cmd"))
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}

	waitForCondition(t, func() bool {
		store := fakeStorageFor(rt, groupID)
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.saveCount > 0 && store.lastApplied >= store.lastSavedIndex
	})
}

func TestProposeRejectsFollower(t *testing.T) {
	cluster := newAsyncTestCluster(t, []NodeID{1, 2, 3}, asyncNetworkConfig{
		MaxDelay: 5 * time.Millisecond,
		Seed:     11,
	})
	groupID := GroupID(12)

	cluster.bootstrapGroup(t, groupID, []NodeID{1, 2, 3})
	cluster.waitForBootstrapApplied(t, groupID, 3)

	leaderID := cluster.waitForLeader(t, groupID)
	followerID := cluster.pickFollower(leaderID)

	fut, err := cluster.runtime(followerID).Propose(context.Background(), groupID, []byte("set follower=1"))
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got future=%v err=%v", fut, err)
	}
}

func TestChangeConfigRejectsFollower(t *testing.T) {
	cluster := newAsyncTestCluster(t, []NodeID{1, 2, 3}, asyncNetworkConfig{
		MaxDelay: 5 * time.Millisecond,
		Seed:     12,
	})
	groupID := GroupID(13)

	cluster.bootstrapGroup(t, groupID, []NodeID{1, 2, 3})
	cluster.waitForBootstrapApplied(t, groupID, 3)

	leaderID := cluster.waitForLeader(t, groupID)
	followerID := cluster.pickFollower(leaderID)

	fut, err := cluster.runtime(followerID).ChangeConfig(context.Background(), groupID, ConfigChange{
		Type:   AddLearner,
		NodeID: 4,
	})
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got future=%v err=%v", fut, err)
	}
}

func TestFatalGroupRejectsFutureOperations(t *testing.T) {
	rt := newStartedRuntime(t)
	groupID := GroupID(14)
	fatalErr := errors.New("fatal apply")
	fsm := &internalFakeStateMachine{applyErr: fatalErr}

	err := rt.BootstrapGroup(context.Background(), BootstrapGroupRequest{
		Group: GroupOptions{
			ID:           groupID,
			Storage:      &internalFakeStorage{},
			StateMachine: fsm,
		},
		Voters: []NodeID{1},
	})
	if err != nil {
		t.Fatalf("BootstrapGroup() error = %v", err)
	}

	waitForCondition(t, func() bool {
		st, err := rt.Status(groupID)
		return err == nil && st.Role == RoleLeader
	})

	fut, err := rt.Propose(context.Background(), groupID, []byte("boom"))
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := fut.Wait(ctx); !errors.Is(err, fatalErr) {
		t.Fatalf("Wait() error = %v, want %v", err, fatalErr)
	}

	if err := rt.Step(context.Background(), Envelope{
		GroupID: groupID,
		Message: raftpb.Message{Type: raftpb.MsgHeartbeat, From: 2, To: 1},
	}); !errors.Is(err, fatalErr) {
		t.Fatalf("Step() error = %v, want %v", err, fatalErr)
	}

	if fut, err := rt.Propose(context.Background(), groupID, []byte("again")); !errors.Is(err, fatalErr) {
		t.Fatalf("Propose() after fatal = future=%v err=%v, want %v", fut, err, fatalErr)
	}

	if fut, err := rt.ChangeConfig(context.Background(), groupID, ConfigChange{
		Type:   AddLearner,
		NodeID: 2,
	}); !errors.Is(err, fatalErr) {
		t.Fatalf("ChangeConfig() after fatal = future=%v err=%v, want %v", fut, err, fatalErr)
	}

	if err := rt.TransferLeadership(context.Background(), groupID, 2); !errors.Is(err, fatalErr) {
		t.Fatalf("TransferLeadership() error = %v, want %v", err, fatalErr)
	}

	if _, err := rt.Status(groupID); !errors.Is(err, fatalErr) {
		t.Fatalf("Status() error = %v, want %v", err, fatalErr)
	}
}

func openSingleNodeLeader(t *testing.T, rt *Runtime, id GroupID) GroupID {
	t.Helper()

	err := rt.BootstrapGroup(context.Background(), BootstrapGroupRequest{
		Group: GroupOptions{
			ID:           id,
			Storage:      &internalFakeStorage{},
			StateMachine: &internalFakeStateMachine{},
		},
		Voters: []NodeID{1},
	})
	if err != nil {
		t.Fatalf("BootstrapGroup() error = %v", err)
	}

	waitForCondition(t, func() bool {
		st, err := rt.Status(id)
		return err == nil && st.Role == RoleLeader
	})
	return id
}

func fakeStorageFor(rt *Runtime, id GroupID) *internalFakeStorage {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	g := rt.groups[id]
	if g == nil {
		return nil
	}
	store, _ := g.storage.(*internalFakeStorage)
	return store
}
