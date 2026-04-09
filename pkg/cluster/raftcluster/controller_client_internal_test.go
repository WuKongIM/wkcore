package raftcluster

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/storage/controllermeta"
	"github.com/WuKongIM/WuKongIM/pkg/transport/nodetransport"
	"github.com/stretchr/testify/require"
)

type controllerClientTestDiscovery struct {
	addrs map[uint64]string
}

func (d *controllerClientTestDiscovery) Resolve(nodeID uint64) (string, error) {
	addr, ok := d.addrs[nodeID]
	if !ok {
		return "", nodetransport.ErrNodeNotFound
	}
	return addr, nil
}

func TestClusterGetReconcileTaskFallsThroughSlowStaleLeaderToCurrentLeader(t *testing.T) {
	slowFollower := nodetransport.NewServer()
	slowFollowerMux := nodetransport.NewRPCMux()
	slowFollowerMux.Handle(rpcServiceController, func(_ context.Context, body []byte) ([]byte, error) {
		var req controllerRPCRequest
		require.NoError(t, json.Unmarshal(body, &req))
		require.Equal(t, controllerRPCGetTask, req.Kind)

		time.Sleep(controllerRequestTimeout + 250*time.Millisecond)
		return json.Marshal(controllerRPCResponse{
			NotLeader: true,
			LeaderID:  2,
		})
	})
	slowFollower.HandleRPCMux(slowFollowerMux)
	require.NoError(t, slowFollower.Start("127.0.0.1:0"))
	t.Cleanup(slowFollower.Stop)

	leader := nodetransport.NewServer()
	leaderMux := nodetransport.NewRPCMux()
	leaderMux.Handle(rpcServiceController, func(_ context.Context, body []byte) ([]byte, error) {
		var req controllerRPCRequest
		require.NoError(t, json.Unmarshal(body, &req))
		require.Equal(t, controllerRPCGetTask, req.Kind)

		task := controllermeta.ReconcileTask{
			GroupID:   req.GroupID,
			Kind:      controllermeta.TaskKindRepair,
			Step:      controllermeta.TaskStepAddLearner,
			Attempt:   2,
			Status:    controllermeta.TaskStatusRetrying,
			NextRunAt: time.Now().Add(time.Second),
			LastError: "injected repair failure",
		}
		return json.Marshal(controllerRPCResponse{Task: &task})
	})
	leader.HandleRPCMux(leaderMux)
	require.NoError(t, leader.Start("127.0.0.1:0"))
	t.Cleanup(leader.Stop)

	discovery := &controllerClientTestDiscovery{
		addrs: map[uint64]string{
			1: slowFollower.Listener().Addr().String(),
			2: leader.Listener().Addr().String(),
		},
	}
	pool := nodetransport.NewPool(discovery, 1, 50*time.Millisecond)
	t.Cleanup(pool.Close)

	client := nodetransport.NewClient(pool)
	t.Cleanup(client.Stop)

	cluster := &Cluster{
		cfg:       Config{NodeID: 3},
		fwdClient: client,
	}
	controllerClient := newControllerClient(cluster, []NodeConfig{
		{NodeID: 1},
		{NodeID: 2},
	}, nil)
	controllerClient.setLeader(1)
	cluster.controllerClient = controllerClient

	ctx, cancel := context.WithTimeout(context.Background(), controllerLeaderWaitTimeout)
	defer cancel()

	task, err := cluster.GetReconcileTask(ctx, 9)
	require.NoError(t, err)
	require.Equal(t, uint32(9), task.GroupID)
	require.Equal(t, uint32(2), task.Attempt)
	require.Equal(t, controllermeta.TaskStatusRetrying, task.Status)
}
