package raftstore

import "encoding/binary"

const (
	keyKindGroupMeta       byte = 0x01
	keyTypeHardState       byte = 0x01
	keyTypeAppliedIndex    byte = 0x02
	keyTypeSnapshot        byte = 0x03
	keyTypeGroupState      byte = 0x04
	keyTypeEntry           byte = 0x10
	keyTypeTruncatedState  byte = 0x11
)

func encodeGroupMetaKey(group uint64, keyType byte) []byte {
	key := make([]byte, 0, 1+8+1)
	key = append(key, keyKindGroupMeta)
	key = binary.BigEndian.AppendUint64(key, group)
	key = append(key, keyType)
	return key
}

func encodeHardStateKey(group uint64) []byte {
	return encodeGroupMetaKey(group, keyTypeHardState)
}

func encodeAppliedIndexKey(group uint64) []byte {
	return encodeGroupMetaKey(group, keyTypeAppliedIndex)
}

func encodeSnapshotKey(group uint64) []byte {
	return encodeGroupMetaKey(group, keyTypeSnapshot)
}

func encodeGroupStateKey(group uint64) []byte {
	return encodeGroupMetaKey(group, keyTypeGroupState)
}

func encodeEntryPrefix(group uint64) []byte {
	key := make([]byte, 0, 1+8+1)
	key = append(key, keyKindGroupMeta)
	key = binary.BigEndian.AppendUint64(key, group)
	key = append(key, keyTypeEntry)
	return key
}

func encodeEntryPrefixEnd(group uint64) []byte {
	return nextPrefix(encodeEntryPrefix(group))
}

func encodeEntryKey(group, index uint64) []byte {
	key := make([]byte, 0, 1+8+1+8)
	key = append(key, encodeEntryPrefix(group)...)
	key = binary.BigEndian.AppendUint64(key, index)
	return key
}

func nextPrefix(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] == 0xff {
			continue
		}
		end[i]++
		return end[:i+1]
	}
	return []byte{0xff}
}
