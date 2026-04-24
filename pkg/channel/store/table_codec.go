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

const messageFamilyCodecVersion byte = 1

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
	primary = append(primary, messageFamilyCodecVersion)
	primary = appendFamilyColumn(primary, messageColumnIDMessageID, encodeFamilyUintBytes(row.MessageID))
	primary = appendFamilyColumn(primary, messageColumnIDFramerFlags, encodeFamilyUintBytes(uint64(row.FramerFlags)))
	primary = appendFamilyColumn(primary, messageColumnIDSetting, encodeFamilyUintBytes(uint64(row.Setting)))
	primary = appendFamilyColumn(primary, messageColumnIDStreamFlag, encodeFamilyUintBytes(uint64(row.StreamFlag)))
	primary = appendFamilyColumn(primary, messageColumnIDMsgKey, []byte(row.MsgKey))
	primary = appendFamilyColumn(primary, messageColumnIDExpire, encodeFamilyUintBytes(uint64(row.Expire)))
	primary = appendFamilyColumn(primary, messageColumnIDClientSeq, encodeFamilyUintBytes(row.ClientSeq))
	primary = appendFamilyColumn(primary, messageColumnIDClientMsgNo, []byte(row.ClientMsgNo))
	primary = appendFamilyColumn(primary, messageColumnIDStreamNo, []byte(row.StreamNo))
	primary = appendFamilyColumn(primary, messageColumnIDStreamID, encodeFamilyUintBytes(row.StreamID))
	primary = appendFamilyColumn(primary, messageColumnIDTimestamp, encodeFamilyIntBytes(int64(row.Timestamp)))
	primary = appendFamilyColumn(primary, messageColumnIDChannelID, []byte(row.ChannelID))
	primary = appendFamilyColumn(primary, messageColumnIDChannelType, encodeFamilyUintBytes(uint64(row.ChannelType)))
	primary = appendFamilyColumn(primary, messageColumnIDTopic, []byte(row.Topic))
	primary = appendFamilyColumn(primary, messageColumnIDFromUID, []byte(row.FromUID))
	primary = appendFamilyColumn(primary, messageColumnIDPayloadHash, encodeFamilyUintBytes(payloadHash))

	payload := []byte{messageFamilyCodecVersion}
	payload = appendFamilyColumn(payload, messageColumnIDPayload, row.Payload)
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
	if len(payload) == 0 {
		return nil
	}
	if payload[0] != messageFamilyCodecVersion {
		return channel.ErrCorruptValue
	}
	payload = payload[1:]

	for len(payload) > 0 {
		columnID, n := binary.Uvarint(payload)
		if n <= 0 {
			return channel.ErrCorruptValue
		}
		payload = payload[n:]

		length, n := binary.Uvarint(payload)
		if n <= 0 {
			return channel.ErrCorruptValue
		}
		payload = payload[n:]
		if uint64(len(payload)) < length {
			return channel.ErrCorruptValue
		}

		value := payload[:length]
		payload = payload[length:]

		switch uint16(columnID) {
		case messageColumnIDMessageID:
			decoded, err := decodeFamilyUintBytes(value)
			if err != nil {
				return err
			}
			row.MessageID = decoded
		case messageColumnIDFramerFlags:
			decoded, err := decodeFamilyUintBytes(value)
			if err != nil {
				return err
			}
			if decoded > math.MaxUint8 {
				return channel.ErrCorruptValue
			}
			row.FramerFlags = uint8(decoded)
		case messageColumnIDSetting:
			decoded, err := decodeFamilyUintBytes(value)
			if err != nil {
				return err
			}
			if decoded > math.MaxUint8 {
				return channel.ErrCorruptValue
			}
			row.Setting = uint8(decoded)
		case messageColumnIDStreamFlag:
			decoded, err := decodeFamilyUintBytes(value)
			if err != nil {
				return err
			}
			if decoded > math.MaxUint8 {
				return channel.ErrCorruptValue
			}
			row.StreamFlag = uint8(decoded)
		case messageColumnIDMsgKey:
			row.MsgKey = string(value)
		case messageColumnIDExpire:
			decoded, err := decodeFamilyUintBytes(value)
			if err != nil {
				return err
			}
			if decoded > math.MaxUint32 {
				return channel.ErrCorruptValue
			}
			row.Expire = uint32(decoded)
		case messageColumnIDClientSeq:
			decoded, err := decodeFamilyUintBytes(value)
			if err != nil {
				return err
			}
			row.ClientSeq = decoded
		case messageColumnIDClientMsgNo:
			row.ClientMsgNo = string(value)
		case messageColumnIDStreamNo:
			row.StreamNo = string(value)
		case messageColumnIDStreamID:
			decoded, err := decodeFamilyUintBytes(value)
			if err != nil {
				return err
			}
			row.StreamID = decoded
		case messageColumnIDTimestamp:
			decoded, err := decodeFamilyIntBytes(value)
			if err != nil {
				return err
			}
			if decoded < math.MinInt32 || decoded > math.MaxInt32 {
				return channel.ErrCorruptValue
			}
			row.Timestamp = int32(decoded)
		case messageColumnIDChannelID:
			row.ChannelID = string(value)
		case messageColumnIDChannelType:
			decoded, err := decodeFamilyUintBytes(value)
			if err != nil {
				return err
			}
			if decoded > math.MaxUint8 {
				return channel.ErrCorruptValue
			}
			row.ChannelType = uint8(decoded)
		case messageColumnIDTopic:
			row.Topic = string(value)
		case messageColumnIDFromUID:
			row.FromUID = string(value)
		case messageColumnIDPayloadHash:
			decoded, err := decodeFamilyUintBytes(value)
			if err != nil {
				return err
			}
			row.PayloadHash = decoded
		case messageColumnIDPayload:
			row.Payload = append([]byte(nil), value...)
		default:
			// Skip unknown columns for forward compatibility.
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

func appendFamilyColumn(dst []byte, columnID uint16, value []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(columnID))
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func encodeFamilyUintBytes(value uint64) []byte {
	return binary.AppendUvarint(nil, value)
}

func decodeFamilyUintBytes(value []byte) (uint64, error) {
	decoded, n := binary.Uvarint(value)
	if n <= 0 || n != len(value) {
		return 0, channel.ErrCorruptValue
	}
	return decoded, nil
}

func encodeFamilyIntBytes(value int64) []byte {
	return binary.AppendUvarint(nil, encodeZigZagInt64(value))
}

func decodeFamilyIntBytes(value []byte) (int64, error) {
	decoded, err := decodeFamilyUintBytes(value)
	if err != nil {
		return 0, err
	}
	return decodeZigZagInt64(decoded), nil
}

func encodeZigZagInt64(v int64) uint64 {
	return uint64(v<<1) ^ uint64(v>>63)
}

func decodeZigZagInt64(v uint64) int64 {
	return int64(v>>1) ^ -int64(v&1)
}
