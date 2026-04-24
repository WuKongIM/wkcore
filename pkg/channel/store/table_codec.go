package store

import (
	"encoding/binary"
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

const (
	keyspaceLog         byte = 0x10
	keyspaceCheckpoint  byte = 0x11
	keyspaceHistory     byte = 0x12
	keyspaceSnapshot    byte = 0x13
	keyspaceIdempotency byte = 0x14
	keyspaceTableState  byte = 0x15
	keyspaceTableIndex  byte = 0x16
	keyspaceTableSystem byte = 0x17
)

const (
	familyValueTypeBytes byte = iota + 1
	familyValueTypeInt
	familyValueTypeUint
)

// messageIndexHit stores the row pointer returned by a secondary message index.
type messageIndexHit struct {
	MessageSeq  uint64
	MessageID   uint64
	PayloadHash uint64
}

func encodeKeyspacePrefix(keyspace byte, channelKey channel.ChannelKey) []byte {
	key := make([]byte, 0, 2+len(channelKey))
	key = append(key, keyspace)
	key = binary.AppendUvarint(key, uint64(len(channelKey)))
	key = append(key, channelKey...)
	return key
}

func encodeTableStatePrefix(channelKey channel.ChannelKey, tableID uint32) []byte {
	key := encodeKeyspacePrefix(keyspaceTableState, channelKey)
	return binary.BigEndian.AppendUint32(key, tableID)
}

func encodeTableStateKey(channelKey channel.ChannelKey, tableID uint32, primaryKey uint64, familyID uint16) []byte {
	key := encodeTableStatePrefix(channelKey, tableID)
	key = binary.BigEndian.AppendUint64(key, primaryKey)
	return binary.BigEndian.AppendUint16(key, familyID)
}

func encodeTableIndexPrefix(channelKey channel.ChannelKey, tableID uint32, indexID uint16) []byte {
	key := encodeKeyspacePrefix(keyspaceTableIndex, channelKey)
	key = binary.BigEndian.AppendUint32(key, tableID)
	return binary.BigEndian.AppendUint16(key, indexID)
}

func encodeTableSystemKey(channelKey channel.ChannelKey, tableID uint32, systemID uint16) []byte {
	key := encodeKeyspacePrefix(keyspaceTableSystem, channelKey)
	key = binary.BigEndian.AppendUint32(key, tableID)
	return binary.BigEndian.AppendUint16(key, systemID)
}

func encodeMessageFamilies(row messageRow) ([]byte, []byte, error) {
	if err := row.validate(); err != nil {
		return nil, nil, err
	}

	payloadHash := row.PayloadHash
	if payloadHash == 0 {
		payloadHash = hashMessagePayload(row.Payload)
	}

	primary := make([]byte, 0, 128)
	primary = appendUintFamilyValue(primary, messageColumnIDMessageID, 0, row.MessageID)
	primary = appendUintFamilyValue(primary, messageColumnIDFramerFlags, messageColumnIDMessageID, uint64(row.FramerFlags))
	primary = appendUintFamilyValue(primary, messageColumnIDSetting, messageColumnIDFramerFlags, uint64(row.Setting))
	primary = appendUintFamilyValue(primary, messageColumnIDStreamFlag, messageColumnIDSetting, uint64(row.StreamFlag))
	primary = appendBytesFamilyValue(primary, messageColumnIDMsgKey, messageColumnIDStreamFlag, []byte(row.MsgKey))
	primary = appendUintFamilyValue(primary, messageColumnIDExpire, messageColumnIDMsgKey, uint64(row.Expire))
	primary = appendUintFamilyValue(primary, messageColumnIDClientSeq, messageColumnIDExpire, row.ClientSeq)
	primary = appendBytesFamilyValue(primary, messageColumnIDClientMsgNo, messageColumnIDClientSeq, []byte(row.ClientMsgNo))
	primary = appendBytesFamilyValue(primary, messageColumnIDStreamNo, messageColumnIDClientMsgNo, []byte(row.StreamNo))
	primary = appendUintFamilyValue(primary, messageColumnIDStreamID, messageColumnIDStreamNo, row.StreamID)
	primary = appendIntFamilyValue(primary, messageColumnIDTimestamp, messageColumnIDStreamID, int64(row.Timestamp))
	primary = appendBytesFamilyValue(primary, messageColumnIDChannelID, messageColumnIDTimestamp, []byte(row.ChannelID))
	primary = appendUintFamilyValue(primary, messageColumnIDChannelType, messageColumnIDChannelID, uint64(row.ChannelType))
	primary = appendBytesFamilyValue(primary, messageColumnIDTopic, messageColumnIDChannelType, []byte(row.Topic))
	primary = appendBytesFamilyValue(primary, messageColumnIDFromUID, messageColumnIDTopic, []byte(row.FromUID))
	primary = appendUintFamilyValue(primary, messageColumnIDPayloadHash, messageColumnIDFromUID, payloadHash)

	payload := appendBytesFamilyValue(nil, messageColumnIDPayload, 0, row.Payload)
	return primary, payload, nil
}

func decodeMessageFamilies(messageSeq uint64, primary []byte, payload []byte) (messageRow, error) {
	row := messageRow{MessageSeq: messageSeq}
	if err := decodeMessageFamilyInto(&row, primary); err != nil {
		return messageRow{}, err
	}
	if err := decodeMessageFamilyInto(&row, payload); err != nil {
		return messageRow{}, err
	}
	if row.MessageID == 0 {
		return messageRow{}, channel.ErrCorruptValue
	}
	return row, nil
}

func decodeMessageFamilyInto(row *messageRow, payload []byte) error {
	var columnID uint16
	for len(payload) > 0 {
		tag := payload[0]
		payload = payload[1:]

		delta := uint16(tag >> 4)
		valueType := tag & 0x0f
		if delta == 0 {
			return channel.ErrCorruptValue
		}
		if delta == 15 {
			extra, n := binary.Uvarint(payload)
			if n <= 0 {
				return channel.ErrCorruptValue
			}
			payload = payload[n:]
			delta += uint16(extra)
		}
		columnID += delta

		switch valueType {
		case familyValueTypeBytes:
			value, rest, err := decodeFamilyBytesValue(payload)
			if err != nil {
				return err
			}
			payload = rest
			switch columnID {
			case messageColumnIDMsgKey:
				row.MsgKey = string(value)
			case messageColumnIDClientMsgNo:
				row.ClientMsgNo = string(value)
			case messageColumnIDStreamNo:
				row.StreamNo = string(value)
			case messageColumnIDChannelID:
				row.ChannelID = string(value)
			case messageColumnIDTopic:
				row.Topic = string(value)
			case messageColumnIDFromUID:
				row.FromUID = string(value)
			case messageColumnIDPayload:
				row.Payload = append([]byte(nil), value...)
			}
		case familyValueTypeInt:
			value, rest, err := decodeFamilyIntValue(payload)
			if err != nil {
				return err
			}
			payload = rest
			switch columnID {
			case messageColumnIDTimestamp:
				if value < math.MinInt32 || value > math.MaxInt32 {
					return channel.ErrCorruptValue
				}
				row.Timestamp = int32(value)
			}
		case familyValueTypeUint:
			value, rest, err := decodeFamilyUintValue(payload)
			if err != nil {
				return err
			}
			payload = rest
			switch columnID {
			case messageColumnIDMessageID:
				row.MessageID = value
			case messageColumnIDFramerFlags:
				if value > math.MaxUint8 {
					return channel.ErrCorruptValue
				}
				row.FramerFlags = uint8(value)
			case messageColumnIDSetting:
				if value > math.MaxUint8 {
					return channel.ErrCorruptValue
				}
				row.Setting = uint8(value)
			case messageColumnIDStreamFlag:
				if value > math.MaxUint8 {
					return channel.ErrCorruptValue
				}
				row.StreamFlag = uint8(value)
			case messageColumnIDExpire:
				if value > math.MaxUint32 {
					return channel.ErrCorruptValue
				}
				row.Expire = uint32(value)
			case messageColumnIDClientSeq:
				row.ClientSeq = value
			case messageColumnIDStreamID:
				row.StreamID = value
			case messageColumnIDChannelType:
				if value > math.MaxUint8 {
					return channel.ErrCorruptValue
				}
				row.ChannelType = uint8(value)
			case messageColumnIDPayloadHash:
				row.PayloadHash = value
			}
		default:
			return channel.ErrCorruptValue
		}
	}
	return nil
}

func encodeMessageIDIndexValue(messageSeq uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, messageSeq)
}

func decodeMessageIDIndexValue(value []byte) (uint64, error) {
	if len(value) != 8 {
		return 0, channel.ErrCorruptValue
	}
	return binary.BigEndian.Uint64(value), nil
}

func encodeIdempotencyIndexValue(row messageRow) ([]byte, error) {
	if err := row.validate(); err != nil {
		return nil, err
	}
	payloadHash := row.PayloadHash
	if payloadHash == 0 {
		payloadHash = hashMessagePayload(row.Payload)
	}
	value := make([]byte, 0, 24)
	value = binary.BigEndian.AppendUint64(value, row.MessageSeq)
	value = binary.BigEndian.AppendUint64(value, row.MessageID)
	value = binary.BigEndian.AppendUint64(value, payloadHash)
	return value, nil
}

func decodeIdempotencyIndexValue(value []byte) (messageIndexHit, error) {
	if len(value) != 24 {
		return messageIndexHit{}, channel.ErrCorruptValue
	}
	return messageIndexHit{
		MessageSeq:  binary.BigEndian.Uint64(value[0:8]),
		MessageID:   binary.BigEndian.Uint64(value[8:16]),
		PayloadHash: binary.BigEndian.Uint64(value[16:24]),
	}, nil
}

func keyUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	upper := append([]byte(nil), prefix...)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] == 0xff {
			continue
		}
		upper[i]++
		return upper[:i+1]
	}
	return nil
}

func appendKeyString(dst []byte, value string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	dst = append(dst, value...)
	return dst
}

func decodeKeyString(src []byte) (string, []byte, error) {
	length, n := binary.Uvarint(src)
	if n <= 0 {
		return "", nil, channel.ErrCorruptValue
	}
	src = src[n:]
	if uint64(len(src)) < length {
		return "", nil, channel.ErrCorruptValue
	}
	return string(src[:length]), src[length:], nil
}

func appendBytesFamilyValue(dst []byte, columnID uint16, prevColumnID uint16, value []byte) []byte {
	delta := columnID - prevColumnID
	dst = appendFamilyValueTag(dst, delta, familyValueTypeBytes)
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func appendIntFamilyValue(dst []byte, columnID uint16, prevColumnID uint16, value int64) []byte {
	delta := columnID - prevColumnID
	dst = appendFamilyValueTag(dst, delta, familyValueTypeInt)
	return binary.AppendUvarint(dst, encodeZigZagInt64(value))
}

func appendUintFamilyValue(dst []byte, columnID uint16, prevColumnID uint16, value uint64) []byte {
	delta := columnID - prevColumnID
	dst = appendFamilyValueTag(dst, delta, familyValueTypeUint)
	return binary.AppendUvarint(dst, value)
}

func appendFamilyValueTag(dst []byte, delta uint16, typ byte) []byte {
	if delta < 15 {
		return append(dst, byte(delta<<4)|typ)
	}
	dst = append(dst, byte(15<<4)|typ)
	return binary.AppendUvarint(dst, uint64(delta-15))
}

func decodeFamilyBytesValue(payload []byte) ([]byte, []byte, error) {
	length, n := binary.Uvarint(payload)
	if n <= 0 {
		return nil, nil, channel.ErrCorruptValue
	}
	payload = payload[n:]
	if uint64(len(payload)) < length {
		return nil, nil, channel.ErrCorruptValue
	}
	return payload[:length], payload[length:], nil
}

func decodeFamilyIntValue(payload []byte) (int64, []byte, error) {
	raw, rest, err := decodeFamilyUintValue(payload)
	if err != nil {
		return 0, nil, err
	}
	return decodeZigZagInt64(raw), rest, nil
}

func decodeFamilyUintValue(payload []byte) (uint64, []byte, error) {
	value, n := binary.Uvarint(payload)
	if n <= 0 {
		return 0, nil, channel.ErrCorruptValue
	}
	return value, payload[n:], nil
}

func encodeZigZagInt64(v int64) uint64 {
	return uint64(v<<1) ^ uint64(v>>63)
}

func decodeZigZagInt64(v uint64) int64 {
	return int64(v>>1) ^ -int64(v&1)
}
