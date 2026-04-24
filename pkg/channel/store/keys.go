package store

import (
	"encoding/binary"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

func encodeLogPrefix(channelKey channel.ChannelKey) []byte {
	return encodeKeyspacePrefix(keyspaceLog, channelKey)
}

func encodeLogRecordKey(channelKey channel.ChannelKey, offset uint64) []byte {
	key := encodeLogPrefix(channelKey)
	return binary.BigEndian.AppendUint64(key, offset)
}

func encodeCheckpointKey(channelKey channel.ChannelKey) []byte {
	return encodeKeyspacePrefix(keyspaceCheckpoint, channelKey)
}

func encodeHistoryPrefix(channelKey channel.ChannelKey) []byte {
	return encodeKeyspacePrefix(keyspaceHistory, channelKey)
}

func encodeHistoryKey(channelKey channel.ChannelKey, startOffset uint64) []byte {
	key := encodeHistoryPrefix(channelKey)
	return binary.BigEndian.AppendUint64(key, startOffset)
}

func encodeSnapshotKey(channelKey channel.ChannelKey) []byte {
	return encodeKeyspacePrefix(keyspaceSnapshot, channelKey)
}

func encodeIdempotencyPrefix(channelKey channel.ChannelKey) []byte {
	return encodeKeyspacePrefix(keyspaceIdempotency, channelKey)
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
