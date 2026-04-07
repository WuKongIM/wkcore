package channellog

import (
	"encoding/binary"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

const (
	keyspaceLog        byte = 0x10
	keyspaceCheckpoint byte = 0x11
	keyspaceHistory    byte = 0x12
	keyspaceSnapshot   byte = 0x13
	keyspaceState      byte = 0x14
)

func encodeGroupPrefix(keyspace byte, groupKey isr.GroupKey) []byte {
	key := make([]byte, 0, 2+len(groupKey))
	key = append(key, keyspace)
	key = binary.AppendUvarint(key, uint64(len(groupKey)))
	key = append(key, groupKey...)
	return key
}

func encodeLogPrefix(groupKey isr.GroupKey) []byte {
	return encodeGroupPrefix(keyspaceLog, groupKey)
}

func encodeLogRecordKey(groupKey isr.GroupKey, offset uint64) []byte {
	key := encodeLogPrefix(groupKey)
	return binary.BigEndian.AppendUint64(key, offset)
}

func encodeCheckpointKey(groupKey isr.GroupKey) []byte {
	return encodeGroupPrefix(keyspaceCheckpoint, groupKey)
}

func encodeHistoryPrefix(groupKey isr.GroupKey) []byte {
	return encodeGroupPrefix(keyspaceHistory, groupKey)
}

func encodeHistoryKey(groupKey isr.GroupKey, startOffset uint64) []byte {
	key := encodeHistoryPrefix(groupKey)
	return binary.BigEndian.AppendUint64(key, startOffset)
}

func encodeSnapshotKey(groupKey isr.GroupKey) []byte {
	return encodeGroupPrefix(keyspaceSnapshot, groupKey)
}

func encodeIdempotencyPrefix(groupKey isr.GroupKey) []byte {
	return encodeGroupPrefix(keyspaceState, groupKey)
}

func encodeIdempotencyKey(groupKey isr.GroupKey, key IdempotencyKey) []byte {
	encoded := encodeIdempotencyPrefix(groupKey)
	encoded = appendKeyString(encoded, key.FromUID)
	encoded = appendKeyString(encoded, key.ClientMsgNo)
	return encoded
}

func decodeIdempotencyKey(raw []byte, prefix []byte) (IdempotencyKey, error) {
	if len(raw) < len(prefix) || string(raw[:len(prefix)]) != string(prefix) {
		return IdempotencyKey{}, ErrCorruptValue
	}

	rest := raw[len(prefix):]
	senderUID, rest, err := decodeKeyString(rest)
	if err != nil {
		return IdempotencyKey{}, err
	}
	clientMsgNo, rest, err := decodeKeyString(rest)
	if err != nil {
		return IdempotencyKey{}, err
	}
	if len(rest) != 0 {
		return IdempotencyKey{}, ErrCorruptValue
	}
	return IdempotencyKey{
		FromUID:     senderUID,
		ClientMsgNo: clientMsgNo,
	}, nil
}

func decodeLogRecordOffset(key []byte, prefix []byte) (uint64, error) {
	if len(key) != len(prefix)+8 {
		return 0, ErrCorruptValue
	}
	return binary.BigEndian.Uint64(key[len(prefix):]), nil
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
		return "", nil, ErrCorruptValue
	}
	src = src[n:]
	if uint64(len(src)) < length {
		return "", nil, ErrCorruptValue
	}
	return string(src[:length]), src[length:], nil
}
