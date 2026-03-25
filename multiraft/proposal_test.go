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

func TestProposeCorrelatesFutureByCommittedIndex(t *testing.T) {
	rt := newStartedRuntime(t)
	groupID := openSingleNodeLeader(t, rt, 15)

	fut, err := rt.Propose(context.Background(), groupID, []byte("set idx=1"))
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}

	res := waitForFutureResult(t, fut)
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

func TestRemoteCommitDoesNotResolveLocalFuture(t *testing.T) {
	cluster := newAsyncTestCluster(t, []NodeID{1, 2, 3}, asyncNetworkConfig{
		MaxDelay: 5 * time.Millisecond,
		Seed:     13,
	})
	groupID := GroupID(16)

	cluster.bootstrapGroup(t, groupID, []NodeID{1, 2, 3})
	cluster.waitForBootstrapApplied(t, groupID, 3)

	oldLeader := cluster.waitForLeader(t, groupID)
	cluster.partitionNode(oldLeader)

	stale, err := cluster.runtime(oldLeader).Propose(context.Background(), groupID, []byte("stale"))
	if err != nil {
		t.Fatalf("Propose(stale) error = %v", err)
	}

	newLeader := cluster.waitForLeaderAmong(t, groupID, cluster.otherNodes(oldLeader))
	fresh, err := cluster.runtime(newLeader).Propose(context.Background(), groupID, []byte("fresh"))
	if err != nil {
		t.Fatalf("Propose(fresh) error = %v", err)
	}

	freshRes := waitForFutureResult(t, fresh)
	if string(freshRes.Data) != "ok:fresh" {
		t.Fatalf("fresh Wait().Data = %q", freshRes.Data)
	}

	cluster.healNode(oldLeader)
	cluster.waitForNodeCommitIndex(t, oldLeader, groupID, freshRes.Index)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	res, err := stale.Wait(ctx)
	if err == nil {
		t.Fatalf("stale future resolved unexpectedly: result=%+v err=%v", res, err)
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrNotLeader) {
		t.Fatalf("stale future error = %v", err)
	}
}

func TestReadyPersistenceFailureDoesNotAdvance(t *testing.T) {
	rt := newStartedRuntime(t)
	groupID := openSingleNodeLeader(t, rt, 17)
	store := fakeStorageFor(rt, groupID)
	if store == nil {
		t.Fatal("fakeStorageFor() = nil")
	}
	fsm := fakeStateMachineFor(rt, groupID)
	if fsm == nil {
		t.Fatal("fakeStateMachineFor() = nil")
	}

	saveErr := errors.New("save failed")
	store.mu.Lock()
	baselineSaves := store.saveCount
	baselineApplied := store.lastApplied
	store.saveErr = saveErr
	store.mu.Unlock()

	fut, err := rt.Propose(context.Background(), groupID, []byte("persist-fail"))
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := fut.Wait(ctx); !errors.Is(err, saveErr) {
		t.Fatalf("Wait() error = %v, want %v", err, saveErr)
	}

	time.Sleep(100 * time.Millisecond)

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.saveCount != baselineSaves {
		t.Fatalf("Save() count = %d, want %d", store.saveCount, baselineSaves)
	}
	if store.lastApplied != baselineApplied {
		t.Fatalf("MarkApplied() = %d, want %d", store.lastApplied, baselineApplied)
	}

	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	if len(fsm.applied) != 0 {
		t.Fatalf("Apply() count = %d, want 0", len(fsm.applied))
	}
}

func TestApplyFatalStopsGroup(t *testing.T) {
	rt := newStartedRuntime(t)
	groupID := GroupID(18)
	fatalErr := errors.New("fatal apply")
	store := &internalFakeStorage{}
	fsm := &internalFakeStateMachine{applyErr: fatalErr}

	err := rt.BootstrapGroup(context.Background(), BootstrapGroupRequest{
		Group: GroupOptions{
			ID:           groupID,
			Storage:      store,
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

	store.mu.Lock()
	baselineApplied := store.lastApplied
	store.mu.Unlock()

	fut, err := rt.Propose(context.Background(), groupID, []byte("boom"))
	if err != nil {
		t.Fatalf("Propose() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := fut.Wait(ctx); !errors.Is(err, fatalErr) {
		t.Fatalf("Wait() error = %v, want %v", err, fatalErr)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.lastApplied != baselineApplied {
		t.Fatalf("MarkApplied() advanced to %d, want %d", store.lastApplied, baselineApplied)
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
	g := groupFor(rt, id)
	if g == nil {
		return nil
	}
	store, _ := g.storage.(*internalFakeStorage)
	return store
}

func fakeStateMachineFor(rt *Runtime, id GroupID) *internalFakeStateMachine {
	g := groupFor(rt, id)
	if g == nil {
		return nil
	}
	fsm, _ := g.stateMachine.(*internalFakeStateMachine)
	return fsm
}

func groupFor(rt *Runtime, id GroupID) *group {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	g := rt.groups[id]
	return g
}
