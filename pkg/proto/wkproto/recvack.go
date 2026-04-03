package wkproto

import (
	"github.com/WuKongIM/WuKongIM/pkg/proto/wkpacket"
	"github.com/pkg/errors"
)

func decodeRecvack(frame wkpacket.Frame, data []byte, version uint8) (wkpacket.Frame, error) {
	dec := NewDecoder(data)
	recvackPacket := &wkpacket.RecvackPacket{}
	recvackPacket.Framer = frame.(wkpacket.Framer)
	var err error
	// 消息唯一ID
	if recvackPacket.MessageID, err = dec.Int64(); err != nil {
		return nil, errors.Wrap(err, "解码MessageId失败！")
	}
	// 消息唯序列号
	if recvackPacket.MessageSeq, err = decodeMessageSeq(dec, version); err != nil {
		return nil, errors.Wrap(err, "解码MessageSeq失败！")
	}
	return recvackPacket, err
}

func encodeRecvack(recvackPacket *wkpacket.RecvackPacket, enc *Encoder, version uint8) error {
	enc.WriteInt64(recvackPacket.MessageID)
	return encodeMessageSeq(enc, version, recvackPacket.MessageSeq)
}

func encodeRecvackSize(_ *wkpacket.RecvackPacket, version uint8) int {
	return wkpacket.MessageIDByteSize + messageSeqSize(version)
}
