package wkproto

import (
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/pkg/errors"
)

func decodeRecvack(frame wkframe.Frame, data []byte, version uint8) (wkframe.Frame, error) {
	dec := NewDecoder(data)
	recvackPacket := &wkframe.RecvackPacket{}
	recvackPacket.Framer = frame.(wkframe.Framer)
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

func encodeRecvack(recvackPacket *wkframe.RecvackPacket, enc *Encoder, version uint8) error {
	enc.WriteInt64(recvackPacket.MessageID)
	return encodeMessageSeq(enc, version, recvackPacket.MessageSeq)
}

func encodeRecvackSize(_ *wkframe.RecvackPacket, version uint8) int {
	return wkframe.MessageIDByteSize + messageSeqSize(version)
}
