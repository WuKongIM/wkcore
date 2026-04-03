package wkproto

import (
	"github.com/WuKongIM/WuKongIM/pkg/proto/wkpacket"
	"github.com/pkg/errors"
)

func decodeSub(frame wkpacket.Frame, data []byte, version uint8) (wkpacket.Frame, error) {
	dec := NewDecoder(data)

	subPacket := &wkpacket.SubPacket{}
	subPacket.Framer = frame.(wkpacket.Framer)
	var err error
	setting, err := dec.Uint8()
	if err != nil {
		return nil, errors.Wrap(err, "解码消息设置失败！")
	}
	subPacket.Setting = wkpacket.Setting(setting)
	// 客户端消息编号
	if subPacket.SubNo, err = dec.String(); err != nil {
		return nil, errors.Wrap(err, "解码SubNo失败！")
	}
	// 频道ID
	if subPacket.ChannelID, err = dec.String(); err != nil {
		return nil, errors.Wrap(err, "解码ChannelId失败！")
	}
	// 频道类型
	if subPacket.ChannelType, err = dec.Uint8(); err != nil {

		return nil, errors.Wrap(err, "解码ChannelType失败！")
	}
	// 动作
	var action uint8
	if action, err = dec.Uint8(); err != nil {
		return nil, errors.Wrap(err, "解码Action失败！")
	}
	subPacket.Action = wkpacket.Action(action)

	// 参数
	if subPacket.Param, err = dec.String(); err != nil {
		return nil, errors.Wrap(err, "解码Param失败！")
	}

	return subPacket, nil
}

func encodeSub(subPacket *wkpacket.SubPacket, enc *Encoder, _ uint8) error {
	_ = enc.WriteByte(subPacket.Setting.Uint8())
	// 客户端消息编号
	enc.WriteString(subPacket.SubNo)
	// 频道ID
	enc.WriteString(subPacket.ChannelID)
	// 频道类型
	enc.WriteUint8(subPacket.ChannelType)
	// 动作
	enc.WriteUint8(subPacket.Action.Uint8())
	// 参数
	enc.WriteString(subPacket.Param)
	return nil
}

func encodeSubSize(subPacket *wkpacket.SubPacket, _ uint8) int {
	var size = 0
	size += wkpacket.SettingByteSize
	size += len(subPacket.SubNo) + wkpacket.StringFixLenByteSize
	size += len(subPacket.ChannelID) + wkpacket.StringFixLenByteSize
	size += wkpacket.ChannelTypeByteSize
	size += wkpacket.ActionByteSize
	size += len(subPacket.Param) + wkpacket.StringFixLenByteSize
	return size
}
