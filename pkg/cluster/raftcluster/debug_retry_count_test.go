package raftcluster_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
)

func TestDebugRetryExecutionCount(t *testing.T) {
	for i := 0; i < 6; i++ {
		nodes := startFourNodesWithController(t, 1, 3)
		var execCount atomic.Int32
		var execMu sync.Mutex
		execAttempts := make([]uint32, 0, 4)
		restore := raftcluster.SetManagedGroupExecutionTestHook(func(groupID uint32, task controllermeta.ReconcileTask) error {
			if groupID == 1 && task.Kind == controllermeta.TaskKindRepair {
				execCount.Add(1)
				execMu.Lock()
				execAttempts = append(execAttempts, task.Attempt)
				execMu.Unlock()
				return errors.New("injected repair failure")
			}
			return nil
		})
		_ = requireEventuallyNoFail(15*time.Second, 200*time.Millisecond, func() bool {
			controller, ok := currentControllerLeaderNode(nodes)
			if !ok {
				return false
			}
			return controller.cluster.MarkNodeDraining(context.Background(), 2) == nil
		})
		gotAttempt2 := requireEventuallyNoFail(12*time.Second, 100*time.Millisecond, func() bool {
			controller, ok := currentControllerLeaderNode(nodes)
			if !ok {
				return false
			}
			task, err := controller.cluster.GetReconcileTask(context.Background(), 1)
			return err == nil && task.Attempt >= 2
		})
		var task controllermeta.ReconcileTask
		var taskErr error
		if controller, ok := currentControllerLeaderNode(nodes); ok {
			task, taskErr = controller.cluster.GetReconcileTask(context.Background(), 1)
		}
		execMu.Lock()
		attempts := append([]uint32(nil), execAttempts...)
		execMu.Unlock()
		t.Logf("run=%d execCount=%d execAttempts=%v attempt2=%v task=%+v taskErr=%v", i, execCount.Load(), attempts, gotAttempt2, task, taskErr)
		restore()
		stopNodes(nodes)
	}
}

func TestDebugRetryExecutionExhaustion(t *testing.T) {
	nodes := startFourNodesWithController(t, 1, 3)
	var execMu sync.Mutex
	execAttempts := make([]uint32, 0, 8)
	restore := raftcluster.SetManagedGroupExecutionTestHook(func(groupID uint32, task controllermeta.ReconcileTask) error {
		if groupID == 1 && task.Kind == controllermeta.TaskKindRepair {
			execMu.Lock()
			execAttempts = append(execAttempts, task.Attempt)
			execMu.Unlock()
			return errors.New("injected repair failure")
		}
		return nil
	})
	defer restore()
	defer stopNodes(nodes)

	_ = requireEventuallyNoFail(15*time.Second, 200*time.Millisecond, func() bool {
		controller, ok := currentControllerLeaderNode(nodes)
		if !ok {
			return false
		}
		return controller.cluster.MarkNodeDraining(context.Background(), 2) == nil
	})

	_ = requireEventuallyNoFail(25*time.Second, 200*time.Millisecond, func() bool {
		controller, ok := currentControllerLeaderNode(nodes)
		if !ok {
			return false
		}
		task, err := controller.cluster.GetReconcileTask(context.Background(), 1)
		return err == nil && task.Status == raftcluster.TaskStatusFailed
	})

	var task controllermeta.ReconcileTask
	var taskErr error
	if controller, ok := currentControllerLeaderNode(nodes); ok {
		task, taskErr = controller.cluster.GetReconcileTask(context.Background(), 1)
	}
	execMu.Lock()
	attempts := append([]uint32(nil), execAttempts...)
	execMu.Unlock()
	t.Logf("final execAttempts=%v task=%+v taskErr=%v", attempts, task, taskErr)
}

func requireEventuallyNoFail(timeout, interval time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(interval)
	}
	return false
}
