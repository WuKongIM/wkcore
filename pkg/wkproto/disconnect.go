package wkproto

import (
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	"github.com/pkg/errors"
)

type DisconnectPacket = wkpacket.DisconnectPacket

func decodeDisConnect(frame Frame, data []byte, version uint8) (Frame, error) {
	dec := NewDecoder(data)
	disConnectPacket := &wkpacket.DisconnectPacket{}
	disConnectPacket.Framer = frame.(wkpacket.Framer)
	var err error
	var reasonCode uint8
	if reasonCode, err = dec.Uint8(); err != nil {
		return nil, errors.Wrap(err, "解码reasonCode失败！")
	}
	disConnectPacket.ReasonCode = wkpacket.ReasonCode(reasonCode)

	if disConnectPacket.Reason, err = dec.String(); err != nil {
		return nil, errors.Wrap(err, "解码reason失败！")
	}

	return disConnectPacket, err
}

func encodeDisConnect(disConnectPacket *wkpacket.DisconnectPacket, enc *Encoder, _ uint8) error {
	// 原因代码
	enc.WriteUint8(disConnectPacket.ReasonCode.Byte())
	// 原因
	enc.WriteString(disConnectPacket.Reason)
	return nil
}

func encodeDisConnectSize(packet *wkpacket.DisconnectPacket, _ uint8) int {

	return ReasonCodeByteSize + len(packet.Reason) + StringFixLenByteSize
}
