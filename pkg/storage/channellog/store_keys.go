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
