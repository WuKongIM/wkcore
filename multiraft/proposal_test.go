package multiraft

import (
	"context"
	"testing"
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
