package handler

import (
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/store"
)

const seqReadChunkLimit = 256

func LoadMsg(st *store.ChannelStore, committedHW, seq uint64) (channel.Message, error) {
	if committedHW == 0 || seq == 0 {
		return channel.Message{}, channel.ErrInvalidArgument
	}
	if seq > committedHW {
		return channel.Message{}, channel.ErrMessageNotFound
	}
	records, err := st.ReadOffsets(seq-1, 1, math.MaxInt)
	if err != nil {
		return channel.Message{}, err
	}
	if len(records) == 0 {
		return channel.Message{}, channel.ErrMessageNotFound
	}
	msg, err := decodeMessageRecord(records[0])
	if err != nil {
		return channel.Message{}, err
	}
	return msg, nil
}

func LoadNextRangeMsgs(st *store.ChannelStore, committedHW, startSeq, endSeq uint64, limit int) ([]channel.Message, error) {
	if limit < 0 {
		return nil, channel.ErrInvalidArgument
	}
	if startSeq == 0 {
		startSeq = 1
	}
	if committedHW == 0 || startSeq > committedHW {
		return nil, nil
	}
	maxSeq := committedHW
	if endSeq != 0 && endSeq < maxSeq {
		maxSeq = endSeq
	}
	if startSeq > maxSeq {
		return nil, nil
	}
	return loadRangeMsgs(st, startSeq, maxSeq, limit)
}

func LoadPrevRangeMsgs(st *store.ChannelStore, committedHW, startSeq, endSeq uint64, limit int) ([]channel.Message, error) {
	if startSeq == 0 || limit < 0 {
		return nil, channel.ErrInvalidArgument
	}
	if endSeq != 0 && endSeq > startSeq {
		return nil, channel.ErrInvalidArgument
	}
	if committedHW == 0 {
		return nil, nil
	}
	if startSeq > committedHW {
		startSeq = committedHW
	}
	maxSeq := startSeq
	minSeq := uint64(1)
	if endSeq != 0 {
		minSeq = endSeq + 1
	}
	if limit > 0 {
		windowMin := uint64(1)
		if startSeq >= uint64(limit) {
			windowMin = startSeq - uint64(limit) + 1
		}
		if windowMin > minSeq {
			minSeq = windowMin
		}
	}
	if maxSeq < minSeq {
		return nil, nil
	}
	return loadRangeMsgs(st, minSeq, maxSeq, limit)
}

func loadRangeMsgs(st *store.ChannelStore, startSeq, endSeq uint64, limit int) ([]channel.Message, error) {
	if startSeq > endSeq {
		return nil, nil
	}
	msgs := make([]channel.Message, 0, initialRangeMsgCapacity(limit))
	nextSeq := startSeq
	remaining := limit
	for nextSeq <= endSeq {
		batchLimit := nextSeqReadBatchLimit(nextSeq, endSeq, remaining)
		records, err := st.ReadOffsets(nextSeq-1, batchLimit, math.MaxInt)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			return msgs, nil
		}
		decoded, err := messagesFromLogRecords(records, endSeq, remaining)
		if err != nil {
			return nil, err
		}
		if len(decoded) == 0 {
			return msgs, nil
		}
		msgs = append(msgs, decoded...)
		nextSeq = decoded[len(decoded)-1].MessageSeq + 1
		if remaining > 0 {
			remaining -= len(decoded)
			if remaining <= 0 {
				return msgs, nil
			}
		}
		if len(records) < batchLimit || len(decoded) < len(records) {
			return msgs, nil
		}
	}
	return msgs, nil
}

func messagesFromLogRecords(records []store.LogRecord, endSeq uint64, limit int) ([]channel.Message, error) {
	msgs := make([]channel.Message, 0, len(records))
	remaining := limit
	for _, record := range records {
		seq := record.Offset + 1
		if seq > endSeq {
			break
		}
		msg, err := decodeMessageRecord(record)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
		if remaining > 0 {
			remaining--
			if remaining == 0 {
				break
			}
		}
	}
	return msgs, nil
}

func nextSeqReadBatchLimit(nextSeq, endSeq uint64, remaining int) int {
	batchLimit := seqReadChunkLimit
	remainingSpan := endSeq - nextSeq + 1
	if remainingSpan < uint64(batchLimit) {
		batchLimit = int(remainingSpan)
	}
	if remaining > 0 && remaining < batchLimit {
		batchLimit = remaining
	}
	return batchLimit
}

func initialRangeMsgCapacity(limit int) int {
	if limit <= 0 || limit > seqReadChunkLimit {
		return seqReadChunkLimit
	}
	return limit
}
