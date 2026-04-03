package jsonrpc

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectParamsToProtoDefaultsLatestVersion(t *testing.T) {
	params := ConnectParams{
		ClientKey:  "client-key",
		DeviceID:   "device-id",
		DeviceFlag: DeviceApp,
		UID:        "user-1",
		Token:      "token-1",
	}

	packet := params.ToProto()

	require.NotNil(t, packet)
	assert.Equal(t, uint8(wkframe.LatestVersion), packet.Version)
}

func TestToFrameReturnsWKPacketFrame(t *testing.T) {
	frame, reqID, err := ToFrame(SendRequest{
		BaseRequest: BaseRequest{
			Jsonrpc: jsonRPCVersion,
			Method:  MethodSend,
			ID:      "req-send-1",
		},
		Params: SendParams{
			ChannelID:   "channel-1",
			ChannelType: 2,
			Payload:     []byte("payload"),
		},
	})

	require.NoError(t, err)
	assert.Equal(t, "req-send-1", reqID)

	sendFrame, ok := frame.(*wkframe.SendPacket)
	require.True(t, ok, "expected *wkframe.SendPacket, got %T", frame)
	assert.Equal(t, wkframe.SEND, sendFrame.GetFrameType())
}

func TestToFrameSendRequestPreservesEmptyClientMsgNo(t *testing.T) {
	frame, reqID, err := ToFrame(SendRequest{
		BaseRequest: BaseRequest{
			Jsonrpc: jsonRPCVersion,
			Method:  MethodSend,
			ID:      "req-send-1",
		},
		Params: SendParams{
			ChannelID:   "channel-1",
			ChannelType: 2,
			Payload:     []byte("payload"),
		},
	})

	require.NoError(t, err)
	assert.Equal(t, "req-send-1", reqID)

	sendFrame, ok := frame.(*wkframe.SendPacket)
	require.True(t, ok, "expected *wkframe.SendPacket, got %T", frame)
	assert.Empty(t, sendFrame.ClientMsgNo)
}

func TestSendRequestToProtoPreservesEmptyClientMsgNo(t *testing.T) {
	packet, err := (SendRequest{
		BaseRequest: BaseRequest{
			Jsonrpc: jsonRPCVersion,
			Method:  MethodSend,
			ID:      "req-send-2",
		},
		Params: SendParams{
			ChannelID:   "channel-1",
			ChannelType: 2,
			Payload:     []byte("payload"),
		},
	}).ToProto()

	require.NoError(t, err)
	require.NotNil(t, packet)
	assert.Empty(t, packet.ClientMsgNo)
}

func TestSendParamsToProtoPreservesEmptyClientMsgNo(t *testing.T) {
	packet := SendParams{
		ChannelID:   "channel-1",
		ChannelType: 2,
		Payload:     []byte("payload"),
	}.ToProto()

	require.NotNil(t, packet)
	assert.Empty(t, packet.ClientMsgNo)
}

func TestFromFrameAcceptsWKPacketConnackPacket(t *testing.T) {
	msg, err := FromFrame("req-connect-1", &wkframe.ConnackPacket{
		Framer: wkframe.Framer{
			HasServerVersion: true,
		},
		ServerVersion: 4,
		ServerKey:     "server-key",
		Salt:          "salt",
		TimeDiff:      12,
		ReasonCode:    wkframe.ReasonSuccess,
		NodeId:        99,
	})

	require.NoError(t, err)

	resp, ok := msg.(ConnectResponse)
	require.True(t, ok, "expected ConnectResponse, got %T", msg)
	require.NotNil(t, resp.Result)
	assert.Equal(t, "req-connect-1", resp.ID)
	assert.Equal(t, 4, resp.Result.ServerVersion)
	assert.Equal(t, ReasonCodeEnum(wkframe.ReasonSuccess), resp.Result.ReasonCode)
	assert.Equal(t, uint64(99), resp.Result.NodeID)
}

func TestFromProtoSendAckPreservesUint64MessageSeq(t *testing.T) {
	result := FromProtoSendAck(&wkframe.SendackPacket{
		MessageID:  99,
		MessageSeq: uint64(^uint32(0)) + 33,
		ReasonCode: wkframe.ReasonSuccess,
	})

	require.NotNil(t, result)
	assert.Equal(t, uint64(^uint32(0))+33, result.MessageSeq)
}

func TestFromProtoRecvPacketMapsStreamIDFromStreamId(t *testing.T) {
	params := FromProtoRecvPacket(&wkframe.RecvPacket{
		StreamNo: "stream-no",
		StreamId: 42,
	})

	assert.Equal(t, "stream-no", params.StreamNo)
	assert.Equal(t, "42", params.StreamID)
}
