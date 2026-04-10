package node

import (
	"context"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	channellog "github.com/WuKongIM/WuKongIM/pkg/channel/log"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/stretchr/testify/require"
)

func TestSubmitCommittedMessageRPCRoutesToOwnerRuntime(t *testing.T) {
	network := newFakeClusterNetwork(
		map[uint64][]uint64{1: {1, 2}},
		map[uint64]uint64{1: 1},
	)
	node1 := network.cluster(1)
	node2 := network.cluster(2)

	recorder := &recordingDeliverySubmit{}
	New(Options{
		Cluster:        node2,
		Presence:       presence.New(presence.Options{}),
		Online:         online.NewRegistry(),
		GatewayBootID:  22,
		DeliverySubmit: recorder,
	})

	client := NewClient(node1)
	err := client.SubmitCommitted(context.Background(), 2, channellog.Message{
		ChannelID:   "u2",
		ChannelType: frame.ChannelTypePerson,
		MessageID:   88,
		MessageSeq:  9,
		FromUID:     "u1",
		ClientMsgNo: "m1",
		Payload:     []byte("hi"),
		ClientSeq:   7,
	})
	require.NoError(t, err)
	require.Equal(t, []channellog.Message{{
		ChannelID:   "u2",
		ChannelType: frame.ChannelTypePerson,
		MessageID:   88,
		MessageSeq:  9,
		FromUID:     "u1",
		ClientMsgNo: "m1",
		Payload:     []byte("hi"),
		ClientSeq:   7,
	}}, recorder.calls)
}

type recordingDeliverySubmit struct {
	calls []channellog.Message
}

func (r *recordingDeliverySubmit) SubmitCommitted(_ context.Context, msg channellog.Message) error {
	copied := msg
	copied.Payload = append([]byte(nil), msg.Payload...)
	r.calls = append(r.calls, copied)
	return nil
}
