package codec

import (
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
	"github.com/pkg/errors"
)

func decodeSendack(f frame.Frame, data []byte, version uint8) (frame.Frame, error) {
	dec := NewDecoder(data)
	sendackPacket := &frame.SendackPacket{}
	sendackPacket.Framer = f.(frame.Framer)
	var err error

	// messageID
	if sendackPacket.MessageID, err = dec.Int64(); err != nil {
		return nil, errors.Wrap(err, "解码MessageId失败！")
	}
	// clientSeq
	var clientSeq uint32
	if clientSeq, err = dec.Uint32(); err != nil {
		return nil, errors.Wrap(err, "解码ClientSeq失败！")
	}
	sendackPacket.ClientSeq = uint64(clientSeq)

	// clientMsgNo
	if sendackPacket.ClientMsgNo, err = dec.String(); err != nil {
		return nil, errors.Wrap(err, "解码ClientMsgNo失败！")
	}

	// messageSeq
	if sendackPacket.MessageSeq, err = decodeMessageSeq(dec, version); err != nil {
		return nil, errors.Wrap(err, "解码MessageSeq失败！")
	}

	// 原因代码
	var reasonCode uint8
	if reasonCode, err = dec.Uint8(); err != nil {
		return nil, errors.Wrap(err, "解码ChannelType失败！")
	}
	sendackPacket.ReasonCode = frame.ReasonCode(reasonCode)

	return sendackPacket, err
}

func encodeSendack(sendackPacket *frame.SendackPacket, enc *Encoder, version uint8) error {
	// 消息唯一ID
	enc.WriteInt64(sendackPacket.MessageID)
	// clientSeq
	enc.WriteUint32(uint32(sendackPacket.ClientSeq))
	// clientMsgNo
	enc.WriteString(sendackPacket.ClientMsgNo)
	// 消息序列号(客户端维护)
	if err := encodeMessageSeq(enc, version, sendackPacket.MessageSeq); err != nil {
		return err
	}
	// 原因代码
	enc.WriteUint8(sendackPacket.ReasonCode.Byte())
	return nil
}

func encodeSendackSize(packet *frame.SendackPacket, version uint8) int {
	return frame.MessageIDByteSize +
		frame.ClientSeqByteSize +
		len(packet.ClientMsgNo) + frame.StringFixLenByteSize +
		messageSeqSize(version) +
		frame.ReasonCodeByteSize
}
