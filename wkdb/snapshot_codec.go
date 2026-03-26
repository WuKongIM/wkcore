package wkdb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

var slotSnapshotMagic = [4]byte{'W', 'K', 'S', 'P'}

const slotSnapshotVersion uint16 = 1

type SlotSnapshot struct {
	SlotID uint64
	Data   []byte
	Stats  SnapshotStats
}

type SnapshotStats struct {
	EntryCount int
	Bytes      int
}

type snapshotEntry struct {
	Key   []byte
	Value []byte
}

type decodedSlotSnapshot struct {
	SlotID  uint64
	Entries []snapshotEntry
	Stats   SnapshotStats
}

func encodeSlotSnapshotPayload(slotID uint64, entries []snapshotEntry) ([]byte, SnapshotStats) {
	data := make([]byte, 0, 32)
	data = append(data, slotSnapshotMagic[:]...)
	data = binary.BigEndian.AppendUint16(data, slotSnapshotVersion)
	data = binary.BigEndian.AppendUint64(data, slotID)
	data = binary.BigEndian.AppendUint64(data, uint64(len(entries)))

	for _, entry := range entries {
		data = binary.AppendUvarint(data, uint64(len(entry.Key)))
		data = binary.AppendUvarint(data, uint64(len(entry.Value)))
		data = append(data, entry.Key...)
		data = append(data, entry.Value...)
	}

	sum := crc32.ChecksumIEEE(data)
	data = binary.BigEndian.AppendUint32(data, sum)
	return data, SnapshotStats{
		EntryCount: len(entries),
		Bytes:      len(data),
	}
}

func decodeSlotSnapshotPayload(data []byte) (decodedSlotSnapshot, error) {
	if len(data) < len(slotSnapshotMagic)+2+8+8+4 {
		return decodedSlotSnapshot{}, ErrCorruptValue
	}

	body := data[:len(data)-4]
	want := binary.BigEndian.Uint32(data[len(data)-4:])
	if got := crc32.ChecksumIEEE(body); got != want {
		return decodedSlotSnapshot{}, ErrChecksumMismatch
	}

	if string(body[:len(slotSnapshotMagic)]) != string(slotSnapshotMagic[:]) {
		return decodedSlotSnapshot{}, ErrCorruptValue
	}
	body = body[len(slotSnapshotMagic):]

	version := binary.BigEndian.Uint16(body[:2])
	if version != slotSnapshotVersion {
		return decodedSlotSnapshot{}, fmt.Errorf("%w: unknown snapshot version %d", ErrCorruptValue, version)
	}
	body = body[2:]

	slotID := binary.BigEndian.Uint64(body[:8])
	body = body[8:]

	entryCount := binary.BigEndian.Uint64(body[:8])
	body = body[8:]

	entries := make([]snapshotEntry, 0, int(entryCount))
	for i := uint64(0); i < entryCount; i++ {
		keyLen, n := binary.Uvarint(body)
		if n <= 0 {
			return decodedSlotSnapshot{}, ErrCorruptValue
		}
		body = body[n:]

		valueLen, n := binary.Uvarint(body)
		if n <= 0 {
			return decodedSlotSnapshot{}, ErrCorruptValue
		}
		body = body[n:]

		if uint64(len(body)) < keyLen+valueLen {
			return decodedSlotSnapshot{}, ErrCorruptValue
		}

		keyEnd := int(keyLen)
		valueEnd := int(valueLen)
		key := append([]byte(nil), body[:keyEnd]...)
		body = body[keyEnd:]
		value := append([]byte(nil), body[:valueEnd]...)
		body = body[valueEnd:]
		entries = append(entries, snapshotEntry{Key: key, Value: value})
	}

	if len(body) != 0 {
		return decodedSlotSnapshot{}, ErrCorruptValue
	}

	return decodedSlotSnapshot{
		SlotID:  slotID,
		Entries: entries,
		Stats: SnapshotStats{
			EntryCount: len(entries),
			Bytes:      len(data),
		},
	}, nil
}
