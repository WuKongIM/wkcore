package wkproto

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	"github.com/stretchr/testify/assert"
)

func TestSendEncodeAndDecode(t *testing.T) {
	var setting wkpacket.Setting
	setting.Set(wkpacket.SettingNoEncrypt)
	packet := &wkpacket.SendPacket{
		Framer: wkpacket.Framer{
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
	packetBytes, err := codec.EncodeFrame(packet, wkpacket.LatestVersion)
	assert.NoError(t, err)

	// 解码
	resultPacket, _, err := codec.DecodeFrame(packetBytes, wkpacket.LatestVersion)
	assert.NoError(t, err)
	resultSendPacket, ok := resultPacket.(*wkpacket.SendPacket)
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
