package wkcodec

import (
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/pkg/errors"
)

func decodeSub(frame wkframe.Frame, data []byte, version uint8) (wkframe.Frame, error) {
	dec := NewDecoder(data)

	subPacket := &wkframe.SubPacket{}
	subPacket.Framer = frame.(wkframe.Framer)
	var err error
	setting, err := dec.Uint8()
	if err != nil {
		return nil, errors.Wrap(err, "解码消息设置失败！")
	}
	subPacket.Setting = wkframe.Setting(setting)
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
	subPacket.Action = wkframe.Action(action)

	// 参数
	if subPacket.Param, err = dec.String(); err != nil {
		return nil, errors.Wrap(err, "解码Param失败！")
	}

	return subPacket, nil
}

func encodeSub(subPacket *wkframe.SubPacket, enc *Encoder, _ uint8) error {
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

func encodeSubSize(subPacket *wkframe.SubPacket, _ uint8) int {
	var size = 0
	size += wkframe.SettingByteSize
	size += len(subPacket.SubNo) + wkframe.StringFixLenByteSize
	size += len(subPacket.ChannelID) + wkframe.StringFixLenByteSize
	size += wkframe.ChannelTypeByteSize
	size += wkframe.ActionByteSize
	size += len(subPacket.Param) + wkframe.StringFixLenByteSize
	return size
}
