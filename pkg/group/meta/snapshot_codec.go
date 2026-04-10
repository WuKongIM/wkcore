package meta

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

type slotSnapshotMeta struct {
	SlotID uint64
	Stats  SnapshotStats
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

func beginSlotSnapshotPayload(slotID uint64) ([]byte, int) {
	data := make([]byte, 0, 32)
	data = append(data, slotSnapshotMagic[:]...)
	data = binary.BigEndian.AppendUint16(data, slotSnapshotVersion)
	data = binary.BigEndian.AppendUint64(data, slotID)
	countPos := len(data)
	data = binary.BigEndian.AppendUint64(data, 0)
	return data, countPos
}

func appendSlotSnapshotEntry(data []byte, key, value []byte) []byte {
	data = binary.AppendUvarint(data, uint64(len(key)))
	data = binary.AppendUvarint(data, uint64(len(value)))
	data = append(data, key...)
	data = append(data, value...)
	return data
}

func finishSlotSnapshotPayload(data []byte, countPos int, entryCount int) ([]byte, SnapshotStats) {
	binary.BigEndian.PutUint64(data[countPos:countPos+8], uint64(entryCount))
	sum := crc32.ChecksumIEEE(data)
	data = binary.BigEndian.AppendUint32(data, sum)
	return data, SnapshotStats{
		EntryCount: entryCount,
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

func readSlotSnapshotMeta(data []byte) (slotSnapshotMeta, error) {
	meta, _, err := parseSlotSnapshotPayload(data)
	if err != nil {
		return slotSnapshotMeta{}, err
	}
	return meta, nil
}

func parseSlotSnapshotPayload(data []byte) (slotSnapshotMeta, []byte, error) {
	if len(data) < len(slotSnapshotMagic)+2+8+8+4 {
		return slotSnapshotMeta{}, nil, ErrCorruptValue
	}

	body := data[:len(data)-4]
	want := binary.BigEndian.Uint32(data[len(data)-4:])
	if got := crc32.ChecksumIEEE(body); got != want {
		return slotSnapshotMeta{}, nil, ErrChecksumMismatch
	}

	if string(body[:len(slotSnapshotMagic)]) != string(slotSnapshotMagic[:]) {
		return slotSnapshotMeta{}, nil, ErrCorruptValue
	}
	body = body[len(slotSnapshotMagic):]

	version := binary.BigEndian.Uint16(body[:2])
	if version != slotSnapshotVersion {
		return slotSnapshotMeta{}, nil, fmt.Errorf("%w: unknown snapshot version %d", ErrCorruptValue, version)
	}
	body = body[2:]

	slotID := binary.BigEndian.Uint64(body[:8])
	body = body[8:]

	entryCount := binary.BigEndian.Uint64(body[:8])
	body = body[8:]

	return slotSnapshotMeta{
		SlotID: slotID,
		Stats: SnapshotStats{
			EntryCount: int(entryCount),
			Bytes:      len(data),
		},
	}, body, nil
}

func visitSlotSnapshotPayload(data []byte, fn func(key, value []byte) error) (slotSnapshotMeta, error) {
	meta, body, err := parseSlotSnapshotPayload(data)
	if err != nil {
		return slotSnapshotMeta{}, err
	}
	if err := visitParsedSlotSnapshotPayload(meta, body, fn); err != nil {
		return slotSnapshotMeta{}, err
	}
	return meta, nil
}

func visitParsedSlotSnapshotPayload(meta slotSnapshotMeta, body []byte, fn func(key, value []byte) error) error {
	for i := 0; i < meta.Stats.EntryCount; i++ {
		keyLen, n := binary.Uvarint(body)
		if n <= 0 {
			return ErrCorruptValue
		}
		body = body[n:]

		valueLen, n := binary.Uvarint(body)
		if n <= 0 {
			return ErrCorruptValue
		}
		body = body[n:]

		if uint64(len(body)) < keyLen+valueLen {
			return ErrCorruptValue
		}

		keyEnd := int(keyLen)
		valueEnd := keyEnd + int(valueLen)
		if err := fn(body[:keyEnd], body[keyEnd:valueEnd]); err != nil {
			return err
		}
		body = body[valueEnd:]
	}

	if len(body) != 0 {
		return ErrCorruptValue
	}
	return nil
}
