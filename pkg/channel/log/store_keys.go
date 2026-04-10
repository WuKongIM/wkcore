package log

import (
	"encoding/binary"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
)

const (
	keyspaceLog        byte = 0x10
	keyspaceCheckpoint byte = 0x11
	keyspaceHistory    byte = 0x12
	keyspaceSnapshot   byte = 0x13
	keyspaceState      byte = 0x14
)

func encodeGroupPrefix(keyspace byte, channelKey isr.ChannelKey) []byte {
	key := make([]byte, 0, 2+len(channelKey))
	key = append(key, keyspace)
	key = binary.AppendUvarint(key, uint64(len(channelKey)))
	key = append(key, channelKey...)
	return key
}

func encodeLogPrefix(channelKey isr.ChannelKey) []byte {
	return encodeGroupPrefix(keyspaceLog, channelKey)
}

func encodeLogRecordKey(channelKey isr.ChannelKey, offset uint64) []byte {
	key := encodeLogPrefix(channelKey)
	return binary.BigEndian.AppendUint64(key, offset)
}

func encodeCheckpointKey(channelKey isr.ChannelKey) []byte {
	return encodeGroupPrefix(keyspaceCheckpoint, channelKey)
}

func encodeHistoryPrefix(channelKey isr.ChannelKey) []byte {
	return encodeGroupPrefix(keyspaceHistory, channelKey)
}

func encodeHistoryKey(channelKey isr.ChannelKey, startOffset uint64) []byte {
	key := encodeHistoryPrefix(channelKey)
	return binary.BigEndian.AppendUint64(key, startOffset)
}

func encodeSnapshotKey(channelKey isr.ChannelKey) []byte {
	return encodeGroupPrefix(keyspaceSnapshot, channelKey)
}

func encodeIdempotencyPrefix(channelKey isr.ChannelKey) []byte {
	return encodeGroupPrefix(keyspaceState, channelKey)
}

func encodeIdempotencyKey(channelKey isr.ChannelKey, key IdempotencyKey) []byte {
	encoded := encodeIdempotencyPrefix(channelKey)
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
