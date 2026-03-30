package wkproto

import (
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	"github.com/pkg/errors"
)

func decodeRecvack(frame wkpacket.Frame, data []byte, _ uint8) (wkpacket.Frame, error) {
	dec := NewDecoder(data)
	recvackPacket := &wkpacket.RecvackPacket{}
	recvackPacket.Framer = frame.(wkpacket.Framer)
	var err error
	// 消息唯一ID
	if recvackPacket.MessageID, err = dec.Int64(); err != nil {
		return nil, errors.Wrap(err, "解码MessageId失败！")
	}
	// 消息唯序列号
	if recvackPacket.MessageSeq, err = dec.Uint32(); err != nil {
		return nil, errors.Wrap(err, "解码MessageSeq失败！")
	}
	return recvackPacket, err
}

func encodeRecvack(recvackPacket *wkpacket.RecvackPacket, enc *Encoder, _ uint8) error {
	enc.WriteInt64(recvackPacket.MessageID)
	enc.WriteUint32(recvackPacket.MessageSeq)
	return nil
}

func encodeRecvackSize(_ *wkpacket.RecvackPacket, _ uint8) int {
	return wkpacket.MessageIDByteSize + wkpacket.MessageSeqByteSize
}
