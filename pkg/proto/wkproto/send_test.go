package wkproto

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSendEncodeAndDecode(t *testing.T) {
	var setting wkframe.Setting
	setting.Set(wkframe.SettingNoEncrypt)
	packet := &wkframe.SendPacket{
		Framer: wkframe.Framer{
			RedDot: true,
		},
		Expire:      100,
		Setting:     setting,
		ClientSeq:   2,
		ChannelID:   "34341",
		ChannelType: 2,
		Payload:     []byte("dsdsdsd1"),
	}
	packet.RedDot = true

	codec := New()
	// 编码
	packetBytes, err := codec.EncodeFrame(packet, wkframe.LatestVersion)
	assert.NoError(t, err)

	// 解码
	resultPacket, _, err := codec.DecodeFrame(packetBytes, wkframe.LatestVersion)
	assert.NoError(t, err)
	resultSendPacket, ok := resultPacket.(*wkframe.SendPacket)
	assert.Equal(t, true, ok)

	// 比较
	assert.Equal(t, packet.ClientSeq, resultSendPacket.ClientSeq)
	assert.Equal(t, packet.ChannelID, resultSendPacket.ChannelID)
	assert.Equal(t, packet.ChannelType, resultSendPacket.ChannelType)
	assert.Equal(t, packet.Expire, resultSendPacket.Expire)
	assert.Equal(t, packet.RedDot, resultSendPacket.RedDot)
	assert.Equal(t, packet.Payload, resultSendPacket.Payload)
	assert.Equal(t, packet.Setting, resultSendPacket.Setting)
}

func TestSendEncodeAndDecodeV5WithStreamDoesNotMiscalculateRemainingLength(t *testing.T) {
	packet := &wkframe.SendPacket{
		Framer: wkframe.Framer{
			RedDot: true,
		},
		Setting:     wkframe.SettingStream,
		ClientSeq:   7,
		ClientMsgNo: "client-msg-no",
		StreamNo:    "stream-no",
		ChannelID:   "channel-1",
		ChannelType: 2,
		MsgKey:      "msg-key",
		Payload:     []byte("payload"),
	}

	codec := New()
	packetBytes, err := codec.EncodeFrame(packet, wkframe.LatestVersion)
	assert.NoError(t, err)

	resultPacket, consumed, err := codec.DecodeFrame(packetBytes, wkframe.LatestVersion)
	assert.NoError(t, err)
	assert.Equal(t, len(packetBytes), consumed)

	resultSendPacket, ok := resultPacket.(*wkframe.SendPacket)
	require.True(t, ok, "expected *wkframe.SendPacket, got %T", resultPacket)
	require.NotNil(t, resultSendPacket)
	assert.Equal(t, packet.ClientSeq, resultSendPacket.ClientSeq)
	assert.Equal(t, packet.ClientMsgNo, resultSendPacket.ClientMsgNo)
	assert.Empty(t, resultSendPacket.StreamNo)
	assert.Equal(t, packet.ChannelID, resultSendPacket.ChannelID)
	assert.Equal(t, packet.ChannelType, resultSendPacket.ChannelType)
	assert.Equal(t, packet.MsgKey, resultSendPacket.MsgKey)
	assert.Equal(t, packet.Payload, resultSendPacket.Payload)
}
