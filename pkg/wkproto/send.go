package wkproto

import (
	"github.com/WuKongIM/WuKongIM/pkg/wkpacket"
	"github.com/pkg/errors"
)

func decodeSend(frame wkpacket.Frame, data []byte, version uint8) (wkpacket.Frame, error) {
	dec := NewDecoder(data)
	sendPacket := &wkpacket.SendPacket{}
	sendPacket.Framer = frame.(wkpacket.Framer)

	var err error
	setting, err := dec.Uint8()
	if err != nil {
		return nil, errors.Wrap(err, "解码消息设置失败！")
	}
	sendPacket.Setting = wkpacket.Setting(setting)

	// 消息序列号(客户端维护)
	var clientSeq uint32
	if clientSeq, err = dec.Uint32(); err != nil {
		return nil, errors.Wrap(err, "解码ClientSeq失败！")
	}
	sendPacket.ClientSeq = uint64(clientSeq)
	// // 客户端唯一标示
	if sendPacket.ClientMsgNo, err = dec.String(); err != nil {
		return nil, errors.Wrap(err, "解码ClientMsgNo失败！")
	}

	// 是否开启了stream
	if version < 5 { // 5版本后不再支持send里不再需要streamNo
		if version >= 2 && sendPacket.Setting.IsSet(wkpacket.SettingStream) {
			// 流式编号
			if sendPacket.StreamNo, err = dec.String(); err != nil {
				return nil, errors.Wrap(err, "解码StreamNo失败！")
			}
		}
	}

	// 频道ID
	if sendPacket.ChannelID, err = dec.String(); err != nil {
		return nil, errors.Wrap(err, "解码ChannelId失败！")
	}
	// 频道类型
	if sendPacket.ChannelType, err = dec.Uint8(); err != nil {

		return nil, errors.Wrap(err, "解码ChannelType失败！")
	}
	// 消息过期时间
	if version >= 3 {
		if sendPacket.Expire, err = dec.Uint32(); err != nil {
			return nil, errors.Wrap(err, "解码Expire失败！")
		}
	}

	// msg key
	if sendPacket.MsgKey, err = dec.String(); err != nil {
		return nil, errors.Wrap(err, "解码MsgKey失败！")
	}
	if sendPacket.Setting.IsSet(wkpacket.SettingTopic) {
		// topic
		if sendPacket.Topic, err = dec.String(); err != nil {
			return nil, errors.Wrap(err, "解密topic消息失败！")
		}
	}
	if sendPacket.Payload, err = dec.BinaryAll(); err != nil {
		return nil, errors.Wrap(err, "解码payload失败！")
	}
	return sendPacket, err
}

func encodeSend(sendPacket *wkpacket.SendPacket, enc *Encoder, version uint8) error {
	_ = enc.WriteByte(sendPacket.Setting.Uint8())
	// 消息序列号(客户端维护)
	enc.WriteUint32(uint32(sendPacket.ClientSeq))
	// 客户端唯一标示
	enc.WriteString(sendPacket.ClientMsgNo)
	// 是否开启了stream
	if version < 5 { // 5版本后不再支持send里不再需要streamNo
		if version >= 2 && sendPacket.Setting.IsSet(wkpacket.SettingStream) {
			// 流式编号
			enc.WriteString(sendPacket.StreamNo)
		}
	}

	// 频道ID
	enc.WriteString(sendPacket.ChannelID)
	// 频道类型
	enc.WriteUint8(sendPacket.ChannelType)
	// 消息过期时间
	if version >= 3 {
		enc.WriteUint32(sendPacket.Expire)
	}
	// msgKey
	enc.WriteString(sendPacket.MsgKey)

	if sendPacket.Setting.IsSet(wkpacket.SettingTopic) {
		enc.WriteString(sendPacket.Topic)
	}
	// 消息内容
	enc.WriteBytes(sendPacket.Payload)

	return nil
}

func encodeSendSize(sendPacket *wkpacket.SendPacket, version uint8) int {
	size := 0
	size += wkpacket.SettingByteSize
	size += wkpacket.ClientSeqByteSize
	size += len(sendPacket.ClientMsgNo) + wkpacket.StringFixLenByteSize
	if version >= 2 && sendPacket.Setting.IsSet(wkpacket.SettingStream) {
		size += len(sendPacket.StreamNo) + wkpacket.StringFixLenByteSize
	}
	size += len(sendPacket.ChannelID) + wkpacket.StringFixLenByteSize
	size += wkpacket.ChannelTypeByteSize
	if version >= 3 {
		size += wkpacket.ExpireByteSize
	}
	size += len(sendPacket.MsgKey) + wkpacket.StringFixLenByteSize
	if sendPacket.Setting.IsSet(wkpacket.SettingTopic) {
		size += len(sendPacket.Topic) + wkpacket.StringFixLenByteSize
	}
	size += len(sendPacket.Payload)
	return size
}
