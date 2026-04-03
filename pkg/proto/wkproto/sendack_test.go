package wkproto

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/stretchr/testify/assert"
)

func TestSendackEncodeAndDecode(t *testing.T) {

	packet := &wkframe.SendackPacket{
		ClientSeq:   234,
		ClientMsgNo: "client-msg-no",
		MessageSeq:  2,
		MessageID:   1234,
		ReasonCode:  wkframe.ReasonSuccess,
	}

	codec := New()
	// 编码
	packetBytes, err := codec.EncodeFrame(packet, 1)
	assert.NoError(t, err)
	// 解码
	resultPacket, _, err := codec.DecodeFrame(packetBytes, 1)
	assert.NoError(t, err)
	resultSendackPacket, ok := resultPacket.(*wkframe.SendackPacket)
	assert.Equal(t, true, ok)

	// 比较
	assert.Equal(t, packet.ClientSeq, resultSendackPacket.ClientSeq)
	assert.Equal(t, packet.ClientMsgNo, resultSendackPacket.ClientMsgNo)
	assert.Equal(t, packet.MessageSeq, resultSendackPacket.MessageSeq)
	assert.Equal(t, packet.MessageID, resultSendackPacket.MessageID)
	assert.Equal(t, packet.ReasonCode, resultSendackPacket.ReasonCode)
}

func TestSendackEncodeAndDecodeSupportsUint64MessageSeqOnLatestVersion(t *testing.T) {
	packet := &wkframe.SendackPacket{
		ClientSeq:   234,
		ClientMsgNo: "client-msg-no",
		MessageSeq:  uint64(^uint32(0)) + 7,
		MessageID:   1234,
		ReasonCode:  wkframe.ReasonSuccess,
	}

	codec := New()
	packetBytes, err := codec.EncodeFrame(packet, wkframe.LatestVersion)
	assert.NoError(t, err)

	resultPacket, _, err := codec.DecodeFrame(packetBytes, wkframe.LatestVersion)
	assert.NoError(t, err)
	resultSendackPacket, ok := resultPacket.(*wkframe.SendackPacket)
	assert.Equal(t, true, ok)
	assert.Equal(t, packet.MessageSeq, resultSendackPacket.MessageSeq)
}
