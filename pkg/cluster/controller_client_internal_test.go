package cluster

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	controllermeta "github.com/WuKongIM/WuKongIM/pkg/controller/meta"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
	"github.com/stretchr/testify/require"
)

type controllerClientTestDiscovery struct {
	addrs map[uint64]string
}

func (d *controllerClientTestDiscovery) Resolve(nodeID uint64) (string, error) {
	addr, ok := d.addrs[nodeID]
	if !ok {
		return "", transport.ErrNodeNotFound
	}
	return addr, nil
}

func TestClusterGetReconcileTaskFallsThroughSlowStaleLeaderToCurrentLeader(t *testing.T) {
	slowFollower := transport.NewServer()
	slowFollowerMux := transport.NewRPCMux()
	slowFollowerMux.Handle(rpcServiceController, func(_ context.Context, body []byte) ([]byte, error) {
		req, err := decodeControllerRequest(body)
		require.NoError(t, err)
		require.Equal(t, controllerRPCGetTask, req.Kind)

		time.Sleep(defaultControllerRequestTimeout + 250*time.Millisecond)
		return encodeControllerResponse(req.Kind, controllerRPCResponse{
			NotLeader: true,
			LeaderID:  2,
		})
	})
	slowFollower.HandleRPCMux(slowFollowerMux)
	require.NoError(t, slowFollower.Start("127.0.0.1:0"))
	t.Cleanup(slowFollower.Stop)

	leader := transport.NewServer()
	leaderMux := transport.NewRPCMux()
	leaderMux.Handle(rpcServiceController, func(_ context.Context, body []byte) ([]byte, error) {
		req, err := decodeControllerRequest(body)
		require.NoError(t, err)
		require.Equal(t, controllerRPCGetTask, req.Kind)

		task := controllermeta.ReconcileTask{
			SlotID:    req.SlotID,
			Kind:      controllermeta.TaskKindRepair,
			Step:      controllermeta.TaskStepAddLearner,
			Attempt:   2,
			Status:    controllermeta.TaskStatusRetrying,
			NextRunAt: time.Now().Add(time.Second),
			LastError: "injected repair failure",
		}
		return encodeControllerResponse(req.Kind, controllerRPCResponse{Task: &task})
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
	pool := transport.NewPool(discovery, 1, 50*time.Millisecond)
	t.Cleanup(pool.Close)

	client := transport.NewClient(pool)
	t.Cleanup(client.Stop)

	cluster := &Cluster{
		cfg: Config{NodeID: 3},
		transportResources: transportResources{
			fwdClient: client,
		},
	}
	controllerClient := newControllerClient(cluster, []NodeConfig{
		{NodeID: 1},
		{NodeID: 2},
	}, nil)
	controllerClient.setLeader(1)
	cluster.controllerClient = controllerClient

	ctx, cancel := context.WithTimeout(context.Background(), defaultControllerLeaderWaitTimeout)
	defer cancel()

	task, err := cluster.GetReconcileTask(ctx, 9)
	require.NoError(t, err)
	require.Equal(t, uint32(9), task.SlotID)
	require.Equal(t, uint32(2), task.Attempt)
	require.Equal(t, controllermeta.TaskStatusRetrying, task.Status)
}

func TestControllerClientForceReconcileFallsThroughRawNotLeaderError(t *testing.T) {
	staleLeader := transport.NewServer()
	staleLeaderMux := transport.NewRPCMux()
	staleLeaderMux.Handle(rpcServiceController, func(_ context.Context, body []byte) ([]byte, error) {
		req, err := decodeControllerRequest(body)
		require.NoError(t, err)
		require.Equal(t, controllerRPCForceReconcile, req.Kind)
		return nil, ErrNotLeader
	})
	staleLeader.HandleRPCMux(staleLeaderMux)
	require.NoError(t, staleLeader.Start("127.0.0.1:0"))
	t.Cleanup(staleLeader.Stop)

	leader := transport.NewServer()
	leaderMux := transport.NewRPCMux()
	leaderMux.Handle(rpcServiceController, func(_ context.Context, body []byte) ([]byte, error) {
		req, err := decodeControllerRequest(body)
		require.NoError(t, err)
		require.Equal(t, controllerRPCForceReconcile, req.Kind)
		return encodeControllerResponse(req.Kind, controllerRPCResponse{})
	})
	leader.HandleRPCMux(leaderMux)
	require.NoError(t, leader.Start("127.0.0.1:0"))
	t.Cleanup(leader.Stop)

	discovery := &controllerClientTestDiscovery{
		addrs: map[uint64]string{
			1: staleLeader.Listener().Addr().String(),
			2: leader.Listener().Addr().String(),
		},
	}
	pool := transport.NewPool(discovery, 1, 50*time.Millisecond)
	t.Cleanup(pool.Close)

	client := transport.NewClient(pool)
	t.Cleanup(client.Stop)

	cluster := &Cluster{
		cfg: Config{NodeID: 3},
		transportResources: transportResources{
			fwdClient: client,
		},
	}
	controllerClient := newControllerClient(cluster, []NodeConfig{
		{NodeID: 1},
		{NodeID: 2},
	}, nil)
	controllerClient.setLeader(1)

	err := controllerClient.ForceReconcile(context.Background(), 9)
	require.NoError(t, err)
	require.Equal(t, multiraft.NodeID(2), controllerClient.cachedLeader())
}

func TestControllerClientLeaderCacheUsesAtomicUint64(t *testing.T) {
	field, ok := reflect.TypeOf(controllerClient{}).FieldByName("leader")
	require.True(t, ok)
	require.Equal(t, reflect.TypeOf(atomic.Uint64{}), field.Type)
}
