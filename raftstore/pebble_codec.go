package raftstore

import "encoding/binary"

const (
	keyKindGroupMeta       byte = 0x01
	keyTypeHardState       byte = 0x01
	keyTypeAppliedIndex    byte = 0x02
	keyTypeSnapshot        byte = 0x03
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

func encodeEntryKey(group, index uint64) []byte {
	key := make([]byte, 0, 1+8+1+8)
	key = append(key, keyKindGroupMeta)
	key = binary.BigEndian.AppendUint64(key, group)
	key = append(key, keyTypeEntry)
	key = binary.BigEndian.AppendUint64(key, index)
	return key
}
