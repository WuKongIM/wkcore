package store

import (
	"encoding/binary"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

const (
	keyspaceLog         byte = 0x10
	keyspaceCheckpoint  byte = 0x11
	keyspaceHistory     byte = 0x12
	keyspaceSnapshot    byte = 0x13
	keyspaceIdempotency byte = 0x14
)

func encodeGroupPrefix(keyspace byte, channelKey channel.ChannelKey) []byte {
	key := make([]byte, 0, 2+len(channelKey))
	key = append(key, keyspace)
	key = binary.AppendUvarint(key, uint64(len(channelKey)))
	key = append(key, channelKey...)
	return key
}

func encodeLogPrefix(channelKey channel.ChannelKey) []byte {
	return encodeGroupPrefix(keyspaceLog, channelKey)
}

func encodeLogRecordKey(channelKey channel.ChannelKey, offset uint64) []byte {
	key := encodeLogPrefix(channelKey)
	return binary.BigEndian.AppendUint64(key, offset)
}

func encodeCheckpointKey(channelKey channel.ChannelKey) []byte {
	return encodeGroupPrefix(keyspaceCheckpoint, channelKey)
}

func encodeHistoryPrefix(channelKey channel.ChannelKey) []byte {
	return encodeGroupPrefix(keyspaceHistory, channelKey)
}

func encodeHistoryKey(channelKey channel.ChannelKey, startOffset uint64) []byte {
	key := encodeHistoryPrefix(channelKey)
	return binary.BigEndian.AppendUint64(key, startOffset)
}

func encodeSnapshotKey(channelKey channel.ChannelKey) []byte {
	return encodeGroupPrefix(keyspaceSnapshot, channelKey)
}

func encodeIdempotencyPrefix(channelKey channel.ChannelKey) []byte {
	return encodeGroupPrefix(keyspaceIdempotency, channelKey)
}

func encodeIdempotencyKey(channelKey channel.ChannelKey, key channel.IdempotencyKey) []byte {
	encoded := encodeIdempotencyPrefix(channelKey)
	encoded = appendKeyString(encoded, key.FromUID)
	encoded = appendKeyString(encoded, key.ClientMsgNo)
	return encoded
}

func decodeIdempotencyKey(raw []byte, prefix []byte) (channel.IdempotencyKey, error) {
	if len(raw) < len(prefix) || string(raw[:len(prefix)]) != string(prefix) {
		return channel.IdempotencyKey{}, channel.ErrCorruptValue
	}

	rest := raw[len(prefix):]
	fromUID, rest, err := decodeKeyString(rest)
	if err != nil {
		return channel.IdempotencyKey{}, err
	}
	clientMsgNo, rest, err := decodeKeyString(rest)
	if err != nil {
		return channel.IdempotencyKey{}, err
	}
	if len(rest) != 0 {
		return channel.IdempotencyKey{}, channel.ErrCorruptValue
	}
	return channel.IdempotencyKey{FromUID: fromUID, ClientMsgNo: clientMsgNo}, nil
}

func decodeLogRecordOffset(key []byte, prefix []byte) (uint64, error) {
	if len(key) != len(prefix)+8 {
		return 0, channel.ErrCorruptValue
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
		return "", nil, channel.ErrCorruptValue
	}
	src = src[n:]
	if uint64(len(src)) < length {
		return "", nil, channel.ErrCorruptValue
	}
	return string(src[:length]), src[length:], nil
}
