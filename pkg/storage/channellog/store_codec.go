package channellog

import (
	"encoding/binary"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

const stateSnapshotVersion byte = 1

type stateSnapshotEntry struct {
	FromUID     string
	ClientMsgNo string
	Entry       IdempotencyEntry
}

func encodeCheckpoint(checkpoint isr.Checkpoint) []byte {
	value := make([]byte, 0, 24)
	value = binary.BigEndian.AppendUint64(value, checkpoint.Epoch)
	value = binary.BigEndian.AppendUint64(value, checkpoint.LogStartOffset)
	value = binary.BigEndian.AppendUint64(value, checkpoint.HW)
	return value
}

func decodeCheckpoint(value []byte) (isr.Checkpoint, error) {
	if len(value) != 24 {
		return isr.Checkpoint{}, ErrCorruptValue
	}
	return isr.Checkpoint{
		Epoch:          binary.BigEndian.Uint64(value[0:8]),
		LogStartOffset: binary.BigEndian.Uint64(value[8:16]),
		HW:             binary.BigEndian.Uint64(value[16:24]),
	}, nil
}

func encodeEpochPoint(point isr.EpochPoint) []byte {
	value := make([]byte, 0, 16)
	value = binary.BigEndian.AppendUint64(value, point.Epoch)
	value = binary.BigEndian.AppendUint64(value, point.StartOffset)
	return value
}

func decodeEpochPoint(value []byte) (isr.EpochPoint, error) {
	if len(value) != 16 {
		return isr.EpochPoint{}, ErrCorruptValue
	}
	return isr.EpochPoint{
		Epoch:       binary.BigEndian.Uint64(value[0:8]),
		StartOffset: binary.BigEndian.Uint64(value[8:16]),
	}, nil
}

func encodeIdempotencyEntry(entry IdempotencyEntry) []byte {
	value := make([]byte, 0, 24)
	value = binary.BigEndian.AppendUint64(value, entry.MessageID)
	value = binary.BigEndian.AppendUint64(value, entry.MessageSeq)
	value = binary.BigEndian.AppendUint64(value, entry.Offset)
	return value
}

func decodeIdempotencyEntry(value []byte) (IdempotencyEntry, error) {
	if len(value) != 24 {
		return IdempotencyEntry{}, ErrCorruptValue
	}
	return IdempotencyEntry{
		MessageID:  binary.BigEndian.Uint64(value[0:8]),
		MessageSeq: binary.BigEndian.Uint64(value[8:16]),
		Offset:     binary.BigEndian.Uint64(value[16:24]),
	}, nil
}

func encodeStateSnapshot(entries []stateSnapshotEntry) []byte {
	payload := make([]byte, 0, 1+binary.MaxVarintLen64+len(entries)*64)
	payload = append(payload, stateSnapshotVersion)
	payload = binary.AppendUvarint(payload, uint64(len(entries)))
	for _, entry := range entries {
		payload = appendKeyString(payload, entry.FromUID)
		payload = appendKeyString(payload, entry.ClientMsgNo)
		payload = append(payload, encodeIdempotencyEntry(entry.Entry)...)
	}
	return payload
}

func decodeStateSnapshot(payload []byte) ([]stateSnapshotEntry, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	if payload[0] != stateSnapshotVersion {
		return nil, ErrCorruptValue
	}
	payload = payload[1:]

	count, n := binary.Uvarint(payload)
	if n <= 0 {
		return nil, ErrCorruptValue
	}
	payload = payload[n:]

	entries := make([]stateSnapshotEntry, 0, int(count))
	for i := uint64(0); i < count; i++ {
		senderUID, rest, err := decodeKeyString(payload)
		if err != nil {
			return nil, err
		}
		clientMsgNo, rest, err := decodeKeyString(rest)
		if err != nil {
			return nil, err
		}
		if len(rest) < 24 {
			return nil, ErrCorruptValue
		}
		entry, err := decodeIdempotencyEntry(rest[:24])
		if err != nil {
			return nil, err
		}
		entries = append(entries, stateSnapshotEntry{
			FromUID:     senderUID,
			ClientMsgNo: clientMsgNo,
			Entry:       entry,
		})
		payload = rest[24:]
	}
	if len(payload) != 0 {
		return nil, ErrCorruptValue
	}
	return entries, nil
}
