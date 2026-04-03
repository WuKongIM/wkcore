package wkproto

import (
	"github.com/WuKongIM/WuKongIM/pkg/proto/wkpacket"
	"github.com/pkg/errors"
)

func decodeConnect(frame wkpacket.Frame, data []byte, version uint8) (wkpacket.Frame, error) {
	dec := NewDecoder(data)
	connectPacket := &wkpacket.ConnectPacket{}
	connectPacket.Framer = frame.(wkpacket.Framer)
	var err error
	if connectPacket.Version, err = dec.Uint8(); err != nil {
		return nil, errors.Wrap(err, "解码version失败！")
	}
	var deviceFlag uint8
	if deviceFlag, err = dec.Uint8(); err != nil {
		return nil, errors.Wrap(err, "解码DeviceFlag失败！")
	}
	connectPacket.DeviceFlag = wkpacket.DeviceFlag(deviceFlag)
	// DeviceId
	if connectPacket.DeviceID, err = dec.String(); err != nil {
		return nil, errors.Wrap(err, "解码DeviceId失败！")
	}

	if connectPacket.UID, err = dec.String(); err != nil {
		return nil, errors.Wrap(err, "解码UID失败！")
	}
	if connectPacket.Token, err = dec.String(); err != nil {
		return nil, errors.Wrap(err, "解码Token失败！")
	}
	if connectPacket.ClientTimestamp, err = dec.Int64(); err != nil {
		return nil, errors.Wrap(err, "解码ClientTimestamp失败！")
	}
	if connectPacket.ClientKey, err = dec.String(); err != nil {
		return nil, errors.Wrap(err, "解码ClientKey失败！")
	}
	return connectPacket, err
}

func encodeConnect(connectPacket *wkpacket.ConnectPacket, enc *Encoder, _ uint8) error {
	// 协议版本
	enc.WriteUint8(connectPacket.Version)
	// 设备标示
	enc.WriteUint8(connectPacket.DeviceFlag.ToUint8())
	// DeviceId
	enc.WriteString(connectPacket.DeviceID)
	// 用户uid
	enc.WriteString(connectPacket.UID)
	// 用户token
	enc.WriteString(connectPacket.Token)
	// 客户端时间戳
	enc.WriteInt64(connectPacket.ClientTimestamp)
	// clientKey
	enc.WriteString(connectPacket.ClientKey)

	return nil
}

func encodeConnectSize(connectPacket *wkpacket.ConnectPacket, _ uint8) int {
	var size = 0
	size += wkpacket.VersionByteSize
	size += wkpacket.DeviceFlagByteSize
	size += len(connectPacket.DeviceID) + wkpacket.StringFixLenByteSize
	size += len(connectPacket.UID) + wkpacket.StringFixLenByteSize
	size += len(connectPacket.Token) + wkpacket.StringFixLenByteSize
	size += wkpacket.ClientTimestampByteSize
	size += len(connectPacket.ClientKey) + wkpacket.StringFixLenByteSize
	return size
}
