package cluster

import (
	"context"
	"testing"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	slotcontroller "github.com/WuKongIM/WuKongIM/pkg/controller/plane"
)

const testControllerLeaderWaitTimeout = 25 * time.Millisecond

func TestRetryControllerCommandUsesClusterLeaderWaitTimeoutOverride(t *testing.T) {
	cluster := &Cluster{controllerLeaderWaitTimeout: testControllerLeaderWaitTimeout}

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

func TestAddSlotChoosesNextSlotIDAndCurrentPeers(t *testing.T) {
	var got slotcontroller.AddSlotRequest
	cluster := &Cluster{
		controllerLeaderWaitTimeout: testControllerLeaderWaitTimeout,
		controllerClient: fakeControllerClient{
			assignments: []controllermeta.SlotAssignment{
				{SlotID: 1, DesiredPeers: []uint64{1, 2, 3}},
				{SlotID: 2, DesiredPeers: []uint64{1, 2, 3}},
			},
			addSlotFn: func(_ context.Context, req slotcontroller.AddSlotRequest) error {
				got = req
				return nil
			},
		},
	}

	slotID, err := cluster.AddSlot(context.Background())
	if err != nil {
		t.Fatalf("AddSlot() error = %v", err)
	}
	if slotID != 3 {
		t.Fatalf("AddSlot() slotID = %d, want 3", slotID)
	}
	if got.NewSlotID != 3 {
		t.Fatalf("submitted NewSlotID = %d, want 3", got.NewSlotID)
	}
	if len(got.Peers) != 3 || got.Peers[0] != 1 || got.Peers[1] != 2 || got.Peers[2] != 3 {
		t.Fatalf("submitted peers = %v, want [1 2 3]", got.Peers)
	}
}

func TestRemoveSlotSubmitsControllerCommand(t *testing.T) {
	var got slotcontroller.RemoveSlotRequest
	cluster := &Cluster{
		controllerLeaderWaitTimeout: testControllerLeaderWaitTimeout,
		router:                      NewRouter(NewHashSlotTable(8, 2), 1, nil),
		controllerClient: fakeControllerClient{
			assignments: []controllermeta.SlotAssignment{
				{SlotID: 1, DesiredPeers: []uint64{1, 2, 3}},
				{SlotID: 2, DesiredPeers: []uint64{1, 2, 3}},
			},
			removeSlotFn: func(_ context.Context, req slotcontroller.RemoveSlotRequest) error {
				got = req
				return nil
			},
		},
	}

	if err := cluster.RemoveSlot(context.Background(), 2); err != nil {
		t.Fatalf("RemoveSlot() error = %v", err)
	}
	if got.SlotID != 2 {
		t.Fatalf("submitted SlotID = %d, want 2", got.SlotID)
	}
}

func TestRebalanceStartsMigrationsFromCurrentTable(t *testing.T) {
	table := NewHashSlotTable(12, 3)
	table.Reassign(4, 1)
	table.Reassign(5, 1)
	table.Reassign(8, 1)
	table.Reassign(9, 1)

	var started []slotcontroller.MigrationRequest
	cluster := &Cluster{
		controllerLeaderWaitTimeout: testControllerLeaderWaitTimeout,
		router:                      NewRouter(table, 1, nil),
		controllerClient: fakeControllerClient{
			startMigrationFn: func(_ context.Context, req slotcontroller.MigrationRequest) error {
				started = append(started, req)
				return nil
			},
		},
	}

	plan, err := cluster.Rebalance(context.Background())
	if err != nil {
		t.Fatalf("Rebalance() error = %v", err)
	}
	if len(plan) != 4 {
		t.Fatalf("len(plan) = %d, want 4", len(plan))
	}
	if len(started) != 4 {
		t.Fatalf("started migrations = %d, want 4", len(started))
	}
	for _, req := range started {
		if req.Source != 1 {
			t.Fatalf("migration source = %d, want 1", req.Source)
		}
		if req.Target != 2 && req.Target != 3 {
			t.Fatalf("migration target = %d, want 2 or 3", req.Target)
		}
	}
}
