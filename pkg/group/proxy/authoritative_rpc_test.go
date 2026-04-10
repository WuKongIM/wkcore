package proxy

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
	metafsm "github.com/WuKongIM/WuKongIM/pkg/group/fsm"
	"github.com/WuKongIM/WuKongIM/pkg/group/multiraft"
	"github.com/stretchr/testify/require"
)

func TestHandleRuntimeMetaRPCBeforeClusterStartReturnsNoLeader(t *testing.T) {
	db := openTestDB(t)
	raftDB := openTestRaftDBAt(t, filepath.Join(t.TempDir(), "raft"))

	cluster, err := raftcluster.NewCluster(raftcluster.Config{
		NodeID:             1,
		ListenAddr:         "127.0.0.1:9090",
		GroupCount:         1,
		ControllerReplicaN: 1,
		GroupReplicaN:      1,
		NewStorage: func(groupID multiraft.GroupID) (multiraft.Storage, error) {
			return raftDB.ForGroup(uint64(groupID)), nil
		},
		NewStateMachine: metafsm.NewStateMachineFactory(db),
		Nodes: []raftcluster.NodeConfig{{
			NodeID: 1,
			Addr:   "127.0.0.1:9090",
		}},
	})
	require.NoError(t, err)

	store := New(cluster, db)
	body, err := json.Marshal(runtimeMetaRPCRequest{
		Op:      runtimeMetaRPCList,
		GroupID: 1,
	})
	require.NoError(t, err)

	var respBody []byte
	require.NotPanics(t, func() {
		respBody, err = store.handleRuntimeMetaRPC(context.Background(), body)
	})
	require.NoError(t, err)

	resp, err := decodeRuntimeMetaRPCResponse(respBody)
	require.NoError(t, err)
	require.Equal(t, rpcStatusNoLeader, resp.Status)
}
