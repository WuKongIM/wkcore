package handler

import (
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/store"
)

const seqReadChunkLimit = 256

func LoadMsg(st *store.ChannelStore, seq uint64) (channel.Message, error) {
	if seq == 0 {
		return channel.Message{}, channel.ErrInvalidArgument
	}
	hw, err := committedHW(st)
	if err != nil {
		return channel.Message{}, err
	}
	if seq > hw {
		return channel.Message{}, channel.ErrMessageNotFound
	}
	records, err := st.Read(seq-1, math.MaxInt)
	if err != nil {
		return channel.Message{}, err
	}
	if len(records) == 0 {
		return channel.Message{}, channel.ErrMessageNotFound
	}
	msg, err := decodeMessage(records[0].Payload)
	if err != nil {
		return channel.Message{}, err
	}
	msg.MessageSeq = seq
	return msg, nil
}

func LoadNextRangeMsgs(st *store.ChannelStore, startSeq, endSeq uint64, limit int) ([]channel.Message, error) {
	if limit < 0 {
		return nil, channel.ErrInvalidArgument
	}
	if startSeq == 0 {
		startSeq = 1
	}
	hw, err := committedHW(st)
	if err != nil {
		return nil, err
	}
	if hw == 0 || startSeq > hw {
		return nil, nil
	}
	maxSeq := hw
	if endSeq != 0 && endSeq < maxSeq {
		maxSeq = endSeq
	}
	if startSeq > maxSeq {
		return nil, nil
	}
	return loadRangeMsgs(st, startSeq, maxSeq, limit)
}

func LoadPrevRangeMsgs(st *store.ChannelStore, startSeq, endSeq uint64, limit int) ([]channel.Message, error) {
	if startSeq == 0 || limit < 0 {
		return nil, channel.ErrInvalidArgument
	}
	if endSeq != 0 && endSeq > startSeq {
		return nil, channel.ErrInvalidArgument
	}
	hw, err := committedHW(st)
	if err != nil {
		return nil, err
	}
	if hw == 0 {
		return nil, nil
	}
	if startSeq > hw {
		startSeq = hw
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

func committedHW(st *store.ChannelStore) (uint64, error) {
	checkpoint, err := st.LoadCheckpoint()
	if err == channel.ErrEmptyState {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return checkpoint.HW, nil
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
		records, err := st.Read(nextSeq-1, math.MaxInt)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			return msgs, nil
		}
		if len(records) > batchLimit {
			records = records[:batchLimit]
		}
		for i, record := range records {
			seq := nextSeq + uint64(i)
			if seq > endSeq {
				return msgs, nil
			}
			msg, err := decodeMessage(record.Payload)
			if err != nil {
				return nil, err
			}
			msg.MessageSeq = seq
			msgs = append(msgs, msg)
			if remaining > 0 {
				remaining--
				if remaining == 0 {
					return msgs, nil
				}
			}
		}
		nextSeq += uint64(len(records))
		if len(records) < batchLimit {
			return msgs, nil
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
