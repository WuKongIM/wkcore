package wkproto

import (
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/pkg/errors"
)

func decodeDisConnect(frame wkframe.Frame, data []byte, version uint8) (wkframe.Frame, error) {
	dec := NewDecoder(data)
	disConnectPacket := &wkframe.DisconnectPacket{}
	disConnectPacket.Framer = frame.(wkframe.Framer)
	var err error
	var reasonCode uint8
	if reasonCode, err = dec.Uint8(); err != nil {
		return nil, errors.Wrap(err, "解码reasonCode失败！")
	}
	disConnectPacket.ReasonCode = wkframe.ReasonCode(reasonCode)

	if disConnectPacket.Reason, err = dec.String(); err != nil {
		return nil, errors.Wrap(err, "解码reason失败！")
	}

	return disConnectPacket, err
}

func encodeDisConnect(disConnectPacket *wkframe.DisconnectPacket, enc *Encoder, _ uint8) error {
	// 原因代码
	enc.WriteUint8(disConnectPacket.ReasonCode.Byte())
	// 原因
	enc.WriteString(disConnectPacket.Reason)
	return nil
}

func encodeDisConnectSize(packet *wkframe.DisconnectPacket, _ uint8) int {
	return wkframe.ReasonCodeByteSize + len(packet.Reason) + wkframe.StringFixLenByteSize
}
