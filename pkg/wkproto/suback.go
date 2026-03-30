package wkproto

import (
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	"github.com/pkg/errors"
)

type Action = wkpacket.Action

const (
	Subscribe   Action = wkpacket.Subscribe
	UnSubscribe Action = wkpacket.UnSubscribe
)

type SubackPacket = wkpacket.SubackPacket

func decodeSuback(frame Frame, data []byte, version uint8) (Frame, error) {
	dec := NewDecoder(data)

	subackPacket := &wkpacket.SubackPacket{}
	subackPacket.Framer = frame.(wkpacket.Framer)

	var err error
	// 客户端消息编号
	if subackPacket.SubNo, err = dec.String(); err != nil {
		return nil, errors.Wrap(err, "解码SubNo失败！")
	}
	// 频道ID
	if subackPacket.ChannelID, err = dec.String(); err != nil {
		return nil, errors.Wrap(err, "解码ChannelId失败！")
	}
	// 频道类型
	if subackPacket.ChannelType, err = dec.Uint8(); err != nil {
		return nil, errors.Wrap(err, "解码ChannelType失败！")
	}
	// 动作
	var action uint8
	if action, err = dec.Uint8(); err != nil {
		return nil, errors.Wrap(err, "解码Action失败！")
	}
	subackPacket.Action = wkpacket.Action(action)
	// 原因码
	var reasonCode byte
	if reasonCode, err = dec.Uint8(); err != nil {

		return nil, errors.Wrap(err, "解码ReasonCode失败！")
	}
	subackPacket.ReasonCode = wkpacket.ReasonCode(reasonCode)

	return subackPacket, nil
}

func encodeSuback(frame Frame, enc *Encoder, _ uint8) error {
	subackPacket := frame.(*wkpacket.SubackPacket)
	// 客户端消息编号
	enc.WriteString(subackPacket.SubNo)
	// 频道ID
	enc.WriteString(subackPacket.ChannelID)
	// 频道类型
	enc.WriteUint8(subackPacket.ChannelType)
	// 动作
	enc.WriteUint8(subackPacket.Action.Uint8())
	// 原因码
	enc.WriteUint8(subackPacket.ReasonCode.Byte())
	return nil
}

func encodeSubackSize(frame Frame, _ uint8) int {
	subPacket := frame.(*wkpacket.SubackPacket)
	var size = 0
	size += (len(subPacket.SubNo) + StringFixLenByteSize)
	size += (len(subPacket.ChannelID) + StringFixLenByteSize)
	size += ChannelTypeByteSize
	size += ActionByteSize
	size += ReasonCodeByteSize
	return size
}
