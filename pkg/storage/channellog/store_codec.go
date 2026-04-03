package channellog

import (
	"encoding/binary"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

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
