package node

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/replication/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/transport/nodetransport"
	"github.com/stretchr/testify/require"
)

func TestPresenceRPCClientFollowsNotLeaderRedirect(t *testing.T) {
	network := newFakeClusterNetwork(
		map[uint64][]uint64{1: {1, 2}},
		map[uint64]uint64{1: 2},
	)

	node1 := network.cluster(1)
	node2 := network.cluster(2)

	presence1 := presence.New(presence.Options{LocalNodeID: 1, GatewayBootID: 11})
	presence2 := presence.New(presence.Options{LocalNodeID: 2, GatewayBootID: 22})
	New(Options{Cluster: node1, Presence: presence1, Online: online.NewRegistry(), GatewayBootID: 11})
	New(Options{Cluster: node2, Presence: presence2, Online: online.NewRegistry(), GatewayBootID: 22})

	client := NewClient(node1)
	_, err := client.RegisterAuthoritative(context.Background(), presence.RegisterAuthoritativeCommand{
		GroupID: 1,
		Route: presence.Route{
			UID:         "u1",
			NodeID:      2,
			BootID:      22,
			SessionID:   100,
			DeviceID:    "d1",
			DeviceFlag:  uint8(wkframe.APP),
			DeviceLevel: uint8(wkframe.DeviceLevelMaster),
			Listener:    "tcp",
		},
	})
	require.NoError(t, err)
	require.Empty(t, presence1.EndpointsByUID(context.Background(), "u1"))
	require.Len(t, presence2.EndpointsByUID(context.Background(), "u1"), 1)
}

func TestPresenceRPCRegisterRoundTrip(t *testing.T) {
	network := newFakeClusterNetwork(
		map[uint64][]uint64{1: {1}},
		map[uint64]uint64{1: 1},
	)
	node1 := network.cluster(1)

	presenceApp := presence.New(presence.Options{
		LocalNodeID:   1,
		GatewayBootID: 11,
		Now:           func() time.Time { return time.Unix(200, 0) },
	})
	New(Options{Cluster: node1, Presence: presenceApp, Online: online.NewRegistry(), GatewayBootID: 11})

	client := NewClient(node1)
	result, err := client.RegisterAuthoritative(context.Background(), presence.RegisterAuthoritativeCommand{
		GroupID: 1,
		Route: presence.Route{
			UID:         "u1",
			NodeID:      1,
			BootID:      11,
			SessionID:   100,
			DeviceID:    "d1",
			DeviceFlag:  uint8(wkframe.APP),
			DeviceLevel: uint8(wkframe.DeviceLevelMaster),
			Listener:    "tcp",
		},
	})
	require.NoError(t, err)
	require.Empty(t, result.Actions)
	require.Len(t, presenceApp.EndpointsByUID(context.Background(), "u1"), 1)
}

func TestPresenceRPCUnregisterRoundTrip(t *testing.T) {
	network := newFakeClusterNetwork(
		map[uint64][]uint64{1: {1}},
		map[uint64]uint64{1: 1},
	)
	node1 := network.cluster(1)

	presenceApp := presence.New(presence.Options{
		LocalNodeID:   1,
		GatewayBootID: 11,
		Now:           func() time.Time { return time.Unix(200, 0) },
	})
	New(Options{Cluster: node1, Presence: presenceApp, Online: online.NewRegistry(), GatewayBootID: 11})

	client := NewClient(node1)
	route := presence.Route{
		UID:         "u1",
		NodeID:      1,
		BootID:      11,
		SessionID:   100,
		DeviceID:    "d1",
		DeviceFlag:  uint8(wkframe.APP),
		DeviceLevel: uint8(wkframe.DeviceLevelMaster),
		Listener:    "tcp",
	}
	_, err := client.RegisterAuthoritative(context.Background(), presence.RegisterAuthoritativeCommand{
		GroupID: 1,
		Route:   route,
	})
	require.NoError(t, err)

	require.NoError(t, client.UnregisterAuthoritative(context.Background(), presence.UnregisterAuthoritativeCommand{
		GroupID: 1,
		Route:   route,
	}))
	require.Empty(t, presenceApp.EndpointsByUID(context.Background(), "u1"))
}

func TestPresenceRPCReplayRoundTrip(t *testing.T) {
	network := newFakeClusterNetwork(
		map[uint64][]uint64{1: {1}},
		map[uint64]uint64{1: 1},
	)
	node1 := network.cluster(1)

	presenceApp := presence.New(presence.Options{
		LocalNodeID:   1,
		GatewayBootID: 11,
		Now:           func() time.Time { return time.Unix(200, 0) },
	})
	New(Options{Cluster: node1, Presence: presenceApp, Online: online.NewRegistry(), GatewayBootID: 11})

	client := NewClient(node1)
	err := client.ReplayAuthoritative(context.Background(), presence.ReplayAuthoritativeCommand{
		Lease: presence.GatewayLease{
			GroupID:        1,
			GatewayNodeID:  1,
			GatewayBootID:  11,
			LeaseUntilUnix: 230,
		},
		Routes: []presence.Route{{
			UID:         "u1",
			NodeID:      1,
			BootID:      11,
			SessionID:   100,
			DeviceID:    "d1",
			DeviceFlag:  uint8(wkframe.APP),
			DeviceLevel: uint8(wkframe.DeviceLevelMaster),
			Listener:    "tcp",
		}},
	})
	require.NoError(t, err)
	require.Len(t, presenceApp.EndpointsByUID(context.Background(), "u1"), 1)
}

func TestPresenceRPCApplyActionRoundTrip(t *testing.T) {
	network := newFakeClusterNetwork(
		map[uint64][]uint64{1: {1}},
		map[uint64]uint64{1: 1},
	)
	node1 := network.cluster(1)

	onlineReg := online.NewRegistry()
	require.NoError(t, onlineReg.Register(testOnlineConn(11, "u1", 1)))
	presenceApp := presence.New(presence.Options{
		LocalNodeID:   1,
		GatewayBootID: 11,
		Online:        onlineReg,
	})
	New(Options{Cluster: node1, Presence: presenceApp, Online: onlineReg, GatewayBootID: 11})

	client := NewClient(node1)
	require.NoError(t, client.ApplyRouteAction(context.Background(), presence.RouteAction{
		UID:       "u1",
		NodeID:    1,
		BootID:    11,
		SessionID: 11,
		Kind:      "close",
	}))

	conn, ok := onlineReg.Connection(11)
	require.True(t, ok)
	require.Equal(t, online.LocalRouteStateClosing, conn.State)
}

type fakeClusterNetwork struct {
	mu        sync.Mutex
	peers     map[uint64][]uint64
	leaders   map[uint64]uint64
	muxByNode map[uint64]*nodetransport.RPCMux
}

func newFakeClusterNetwork(peers map[uint64][]uint64, leaders map[uint64]uint64) *fakeClusterNetwork {
	return &fakeClusterNetwork{
		peers:     peers,
		leaders:   leaders,
		muxByNode: make(map[uint64]*nodetransport.RPCMux),
	}
}

func (n *fakeClusterNetwork) cluster(localNodeID uint64) *fakeCluster {
	return &fakeCluster{localNodeID: localNodeID, network: n}
}

func (n *fakeClusterNetwork) mux(nodeID uint64) *nodetransport.RPCMux {
	n.mu.Lock()
	defer n.mu.Unlock()

	if mux := n.muxByNode[nodeID]; mux != nil {
		return mux
	}
	mux := nodetransport.NewRPCMux()
	n.muxByNode[nodeID] = mux
	return mux
}

type fakeCluster struct {
	localNodeID uint64
	network     *fakeClusterNetwork
}

func (c *fakeCluster) RPCMux() *nodetransport.RPCMux {
	return c.network.mux(c.localNodeID)
}

func (c *fakeCluster) LeaderOf(groupID multiraft.GroupID) (multiraft.NodeID, error) {
	leaderID, ok := c.network.leaders[uint64(groupID)]
	if !ok {
		return 0, raftcluster.ErrNoLeader
	}
	return multiraft.NodeID(leaderID), nil
}

func (c *fakeCluster) IsLocal(nodeID multiraft.NodeID) bool {
	return c.localNodeID == uint64(nodeID)
}

func (c *fakeCluster) SlotForKey(key string) multiraft.GroupID {
	return multiraft.GroupID(1)
}

func (c *fakeCluster) RPCService(ctx context.Context, nodeID multiraft.NodeID, groupID multiraft.GroupID, serviceID uint8, payload []byte) ([]byte, error) {
	return c.network.mux(uint64(nodeID)).HandleRPC(ctx, append([]byte{serviceID}, payload...))
}

func (c *fakeCluster) PeersForGroup(groupID multiraft.GroupID) []multiraft.NodeID {
	peers := c.network.peers[uint64(groupID)]
	out := make([]multiraft.NodeID, 0, len(peers))
	for _, peer := range peers {
		out = append(out, multiraft.NodeID(peer))
	}
	return out
}
