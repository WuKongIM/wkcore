package wkdb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	recordKindPrimary        byte = 0x01
	recordKindSecondaryIndex byte = 0x02
	wrappedValueTag          byte = 0x0A
	valueTypeInt             byte = 0x03
	valueTypeBytes           byte = 0x06

	keyspaceState byte = 0x10
	keyspaceIndex byte = 0x11
	keyspaceMeta  byte = 0x12
	keyspaceRaft  byte = 0x20
)

func encodeStatePrefix(slot uint64, tableID uint32) []byte {
	key := make([]byte, 0, 1+8+4)
	key = append(key, keyspaceState)
	key = binary.BigEndian.AppendUint64(key, slot)
	key = binary.BigEndian.AppendUint32(key, tableID)
	return key
}

func encodeIndexPrefix(slot uint64, tableID uint32, indexID uint16) []byte {
	key := make([]byte, 0, 1+8+4+2)
	key = append(key, keyspaceIndex)
	key = binary.BigEndian.AppendUint64(key, slot)
	key = binary.BigEndian.AppendUint32(key, tableID)
	key = binary.BigEndian.AppendUint16(key, indexID)
	return key
}

func encodeMetaPrefix(slot uint64) []byte {
	key := make([]byte, 0, 1+8)
	key = append(key, keyspaceMeta)
	key = binary.BigEndian.AppendUint64(key, slot)
	return key
}

func encodeUserPrimaryKey(slot uint64, uid string, familyID uint16) []byte {
	key := make([]byte, 0, 32)
	key = encodeStatePrefix(slot, UserTable.ID)
	key = appendKeyString(key, uid)
	key = binary.AppendUvarint(key, uint64(familyID))
	return key
}

func encodeChannelPrimaryKey(slot uint64, channelID string, channelType int64, familyID uint16) []byte {
	key := make([]byte, 0, 48)
	key = encodeStatePrefix(slot, ChannelTable.ID)
	key = appendKeyString(key, channelID)
	key = appendKeyInt64Ordered(key, channelType)
	key = binary.AppendUvarint(key, uint64(familyID))
	return key
}

func encodeChannelIDIndexKey(slot uint64, channelID string, channelType int64) []byte {
	key := encodeChannelIDIndexPrefix(slot, channelID)
	key = appendKeyInt64Ordered(key, channelType)
	return key
}

func encodeChannelIDIndexPrefix(slot uint64, channelID string) []byte {
	key := make([]byte, 0, 40)
	key = encodeIndexPrefix(slot, ChannelTable.ID, channelIndexIDChannelID)
	key = appendKeyString(key, channelID)
	return key
}

func encodeUserFamilyValue(token string, deviceFlag, deviceLevel int64, key []byte) []byte {
	payload := make([]byte, 0, 32)
	payload = appendBytesValue(payload, userColumnIDToken, 0, token)
	payload = appendIntValue(payload, userColumnIDDeviceFlag, userColumnIDToken, deviceFlag)
	payload = appendIntValue(payload, userColumnIDDeviceLevel, userColumnIDDeviceFlag, deviceLevel)
	return wrapFamilyValue(key, payload)
}

func decodeUserFamilyValue(key, value []byte) (string, int64, int64, error) {
	_, payload, err := decodeWrappedValue(key, value)
	if err != nil {
		return "", 0, 0, err
	}
	cols, err := decodeFamilyPayload(payload)
	if err != nil {
		return "", 0, 0, err
	}

	token, err := requireStringColumn(cols, userColumnIDToken)
	if err != nil {
		return "", 0, 0, err
	}
	deviceFlag, err := requireInt64Column(cols, userColumnIDDeviceFlag)
	if err != nil {
		return "", 0, 0, err
	}
	deviceLevel, err := requireInt64Column(cols, userColumnIDDeviceLevel)
	if err != nil {
		return "", 0, 0, err
	}
	return token, deviceFlag, deviceLevel, nil
}

func encodeChannelFamilyValue(ban int64, key []byte) []byte {
	payload := make([]byte, 0, 8)
	payload = appendIntValue(payload, channelColumnIDBan, 0, ban)
	return wrapFamilyValue(key, payload)
}

func decodeChannelFamilyValue(key, value []byte) (int64, error) {
	_, payload, err := decodeWrappedValue(key, value)
	if err != nil {
		return 0, err
	}
	cols, err := decodeFamilyPayload(payload)
	if err != nil {
		return 0, err
	}
	ban, err := requireInt64Column(cols, channelColumnIDBan)
	if err != nil {
		return 0, err
	}
	return ban, nil
}

func wrapFamilyValue(key, payload []byte) []byte {
	body := append([]byte{wrappedValueTag}, payload...)
	sum := crc32.ChecksumIEEE(append(append([]byte{}, key...), body...))
	if sum == 0 {
		sum = 1
	}

	value := make([]byte, 4, 4+len(body))
	binary.BigEndian.PutUint32(value[:4], sum)
	value = append(value, body...)
	return value
}

func decodeWrappedValue(key, value []byte) (byte, []byte, error) {
	if len(value) < 5 {
		return 0, nil, fmt.Errorf("wkdb: wrapped value too short")
	}
	want := binary.BigEndian.Uint32(value[:4])
	body := value[4:]
	got := crc32.ChecksumIEEE(append(append([]byte{}, key...), body...))
	if got == 0 {
		got = 1
	}
	if got != want {
		return 0, nil, ErrChecksumMismatch
	}
	if body[0] != wrappedValueTag {
		return 0, nil, fmt.Errorf("%w: unexpected value tag %x", ErrCorruptValue, body[0])
	}
	return body[0], body[1:], nil
}

func decodeFamilyPayload(payload []byte) (map[uint16]any, error) {
	result := make(map[uint16]any)
	var colID uint16

	for len(payload) > 0 {
		tag := payload[0]
		payload = payload[1:]

		delta := uint16(tag >> 4)
		valueType := tag & 0x0f
		if delta == 0 {
			return nil, fmt.Errorf("%w: zero column delta", ErrCorruptValue)
		}
		colID += delta

		switch valueType {
		case valueTypeBytes:
			length, n := binary.Uvarint(payload)
			if n <= 0 {
				return nil, fmt.Errorf("wkdb: invalid bytes length")
			}
			payload = payload[n:]
			if uint64(len(payload)) < length {
				return nil, fmt.Errorf("wkdb: bytes payload truncated")
			}
			result[colID] = string(payload[:length])
			payload = payload[length:]
		case valueTypeInt:
			raw, n := binary.Uvarint(payload)
			if n <= 0 {
				return nil, fmt.Errorf("wkdb: invalid int payload")
			}
			payload = payload[n:]
			result[colID] = decodeZigZagInt64(raw)
		default:
			return nil, fmt.Errorf("wkdb: unsupported value type %d", valueType)
		}
	}

	return result, nil
}

func requireStringColumn(cols map[uint16]any, columnID uint16) (string, error) {
	value, ok := cols[columnID]
	if !ok {
		return "", fmt.Errorf("%w: missing string column %d", ErrCorruptValue, columnID)
	}
	typed, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%w: invalid string column %d", ErrCorruptValue, columnID)
	}
	return typed, nil
}

func requireInt64Column(cols map[uint16]any, columnID uint16) (int64, error) {
	value, ok := cols[columnID]
	if !ok {
		return 0, fmt.Errorf("%w: missing int column %d", ErrCorruptValue, columnID)
	}
	typed, ok := value.(int64)
	if !ok {
		return 0, fmt.Errorf("%w: invalid int column %d", ErrCorruptValue, columnID)
	}
	return typed, nil
}

func appendBytesValue(dst []byte, columnID, prevColumnID uint16, value string) []byte {
	delta := columnID - prevColumnID
	dst = append(dst, encodeValueTag(delta, valueTypeBytes))
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	dst = append(dst, value...)
	return dst
}

func appendIntValue(dst []byte, columnID, prevColumnID uint16, value int64) []byte {
	delta := columnID - prevColumnID
	dst = append(dst, encodeValueTag(delta, valueTypeInt))
	dst = binary.AppendUvarint(dst, encodeZigZagInt64(value))
	return dst
}

func encodeValueTag(delta uint16, typ byte) byte {
	return byte(delta<<4) | typ
}

func appendTableIndexPrefix(dst []byte, kind byte, tableID uint32, indexID uint16) []byte {
	dst = append(dst, kind)
	dst = binary.BigEndian.AppendUint32(dst, tableID)
	dst = binary.BigEndian.AppendUint16(dst, indexID)
	return dst
}

func appendKeyString(dst []byte, value string) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(value)))
	dst = append(dst, value...)
	return dst
}

func appendKeyInt64Ordered(dst []byte, value int64) []byte {
	u := uint64(value) ^ 0x8000000000000000
	return binary.BigEndian.AppendUint64(dst, u)
}

func decodeOrderedInt64(src []byte) (int64, []byte, error) {
	if len(src) < 8 {
		return 0, nil, fmt.Errorf("wkdb: ordered int64 too short")
	}
	u := binary.BigEndian.Uint64(src[:8]) ^ 0x8000000000000000
	return int64(u), src[8:], nil
}

func decodeKeyString(src []byte) (string, []byte, error) {
	if len(src) < 2 {
		return "", nil, fmt.Errorf("wkdb: key string too short")
	}
	n := binary.BigEndian.Uint16(src[:2])
	src = src[2:]
	if len(src) < int(n) {
		return "", nil, fmt.Errorf("wkdb: key string truncated")
	}
	return string(src[:n]), src[n:], nil
}

func encodeZigZagInt64(v int64) uint64 {
	return uint64(v<<1) ^ uint64(v>>63)
}

func decodeZigZagInt64(v uint64) int64 {
	return int64(v>>1) ^ -int64(v&1)
}
