package node

import (
	"context"
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	"github.com/WuKongIM/WuKongIM/pkg/channel"
	raftcluster "github.com/WuKongIM/WuKongIM/pkg/cluster"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/WuKongIM/WuKongIM/pkg/slot/multiraft"
	"github.com/WuKongIM/WuKongIM/pkg/transport"
	"github.com/stretchr/testify/require"
)

func TestAppendToLeaderRPCAppendsOnTargetNode(t *testing.T) {
	network := newFakeClusterNetwork(
		map[uint64][]uint64{1: {1, 2}},
		map[uint64]uint64{1: 1},
	)
	node1 := network.cluster(1)
	node2 := network.cluster(2)

	req := channel.AppendRequest{
		ChannelID: channel.ChannelID{ID: "u2@u1", Type: frame.ChannelTypePerson},
		Message: channel.Message{
			FromUID:     "u1",
			ClientMsgNo: "m1",
			Payload:     []byte("hi"),
		},
	}
	channelLog := &stubNodeChannelLog{
		status: channel.ChannelRuntimeStatus{
			ID:     req.ChannelID,
			Leader: 2,
		},
		appendResult: channel.AppendResult{MessageID: 7, MessageSeq: 8},
	}
	New(Options{
		Cluster:     node2,
		Presence:    presence.New(presence.Options{}),
		Online:      online.NewRegistry(),
		LocalNodeID: 2,
		ChannelLog:  channelLog,
	})

	client := NewClient(node1)
	result, err := client.AppendToLeader(context.Background(), 2, req)

	require.NoError(t, err)
	require.Equal(t, channel.AppendResult{MessageID: 7, MessageSeq: 8}, result)
	require.Equal(t, []channel.AppendRequest{req}, channelLog.appendCalls)
}

func TestAppendToLeaderRPCFollowsLeaderRedirect(t *testing.T) {
	network := newFakeClusterNetwork(
		map[uint64][]uint64{1: {1, 2, 3}},
		map[uint64]uint64{1: 1},
	)
	node1 := network.cluster(1)
	node2 := network.cluster(2)
	node3 := network.cluster(3)

	req := channel.AppendRequest{
		ChannelID: channel.ChannelID{ID: "group-1", Type: frame.ChannelTypeGroup},
		Message: channel.Message{
			FromUID:     "u1",
			ClientMsgNo: "m2",
			Payload:     []byte("hello"),
		},
	}
	redirectLog := &stubNodeChannelLog{
		status: channel.ChannelRuntimeStatus{
			ID:     req.ChannelID,
			Leader: 3,
		},
	}
	leaderLog := &stubNodeChannelLog{
		status: channel.ChannelRuntimeStatus{
			ID:     req.ChannelID,
			Leader: 3,
		},
		appendResult: channel.AppendResult{MessageID: 9, MessageSeq: 10},
	}
	New(Options{
		Cluster:     node2,
		Presence:    presence.New(presence.Options{}),
		Online:      online.NewRegistry(),
		LocalNodeID: 2,
		ChannelLog:  redirectLog,
	})
	New(Options{
		Cluster:     node3,
		Presence:    presence.New(presence.Options{}),
		Online:      online.NewRegistry(),
		LocalNodeID: 3,
		ChannelLog:  leaderLog,
	})

	client := NewClient(node1)
	result, err := client.AppendToLeader(context.Background(), 2, req)

	require.NoError(t, err)
	require.Equal(t, channel.AppendResult{MessageID: 9, MessageSeq: 10}, result)
	require.Empty(t, redirectLog.appendCalls)
	require.Equal(t, []channel.AppendRequest{req}, leaderLog.appendCalls)
}

func TestAppendToLeaderRPCReturnsTypedNotLeaderWhenRemoteLeaderChangesBeforeAppend(t *testing.T) {
	req := channel.AppendRequest{
		ChannelID: channel.ChannelID{ID: "u2@u1", Type: frame.ChannelTypePerson},
		Message: channel.Message{
			FromUID:     "u1",
			ClientMsgNo: "m3",
			Payload:     []byte("hi"),
		},
	}
	client := NewClient(remoteErrorCluster{
		err: fmt.Errorf("nodetransport: remote error: %s", channel.ErrNotLeader),
	})
	_, err := client.AppendToLeader(context.Background(), 2, req)

	require.ErrorIs(t, err, channel.ErrNotLeader)
}

type stubNodeChannelLog struct {
	status       channel.ChannelRuntimeStatus
	statusErr    error
	appendResult channel.AppendResult
	appendErr    error
	appendCalls  []channel.AppendRequest
}

func (s *stubNodeChannelLog) Status(channel.ChannelID) (channel.ChannelRuntimeStatus, error) {
	return s.status, s.statusErr
}

func (s *stubNodeChannelLog) Fetch(context.Context, channel.FetchRequest) (channel.FetchResult, error) {
	return channel.FetchResult{}, nil
}

func (s *stubNodeChannelLog) Append(_ context.Context, req channel.AppendRequest) (channel.AppendResult, error) {
	s.appendCalls = append(s.appendCalls, req)
	return s.appendResult, s.appendErr
}

type remoteErrorCluster struct {
	err error
}

func (c remoteErrorCluster) RPCMux() *transport.RPCMux { return nil }

func (c remoteErrorCluster) LeaderOf(multiraft.SlotID) (multiraft.NodeID, error) {
	return 0, raftcluster.ErrNoLeader
}

func (c remoteErrorCluster) IsLocal(multiraft.NodeID) bool { return false }

func (c remoteErrorCluster) SlotForKey(string) multiraft.SlotID { return 0 }

func (c remoteErrorCluster) RPCService(context.Context, multiraft.NodeID, multiraft.SlotID, uint8, []byte) ([]byte, error) {
	return nil, c.err
}

func (c remoteErrorCluster) PeersForSlot(multiraft.SlotID) []multiraft.NodeID { return nil }
