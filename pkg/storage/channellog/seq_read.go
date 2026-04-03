package channellog

import (
	"errors"
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

const seqReadChunkLimit = 256

func (s *Store) LoadMsg(seq uint64) (ChannelMessage, error) {
	if err := s.validate(); err != nil {
		return ChannelMessage{}, err
	}
	if seq == 0 {
		return ChannelMessage{}, ErrInvalidArgument
	}
	hw, err := s.committedHW()
	if err != nil {
		return ChannelMessage{}, err
	}
	if seq > hw {
		return ChannelMessage{}, ErrMessageNotFound
	}

	records, err := s.readOffsets(seq-1, 1, math.MaxInt)
	if err != nil {
		return ChannelMessage{}, err
	}
	if len(records) == 0 {
		return ChannelMessage{}, ErrMessageNotFound
	}
	return decodeChannelMessage(records[0])
}

func (s *Store) LoadNextRangeMsgs(startSeq, endSeq uint64, limit int) ([]ChannelMessage, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, ErrInvalidArgument
	}
	if startSeq == 0 {
		startSeq = 1
	}

	hw, err := s.committedHW()
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
	return s.loadRangeMsgs(startSeq, maxSeq, limit)
}

func (s *Store) LoadPrevRangeMsgs(startSeq, endSeq uint64, limit int) ([]ChannelMessage, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if startSeq == 0 || limit < 0 {
		return nil, ErrInvalidArgument
	}
	if endSeq != 0 && endSeq > startSeq {
		return nil, ErrInvalidArgument
	}

	hw, err := s.committedHW()
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
	return s.loadRangeMsgs(minSeq, maxSeq, limit)
}

func (s *Store) TruncateLogTo(seq uint64) error {
	if err := s.validate(); err != nil {
		return err
	}
	if seq == 0 {
		return ErrInvalidArgument
	}
	if err := s.truncateOffsets(seq); err != nil {
		return err
	}

	checkpoint, err := s.loadCheckpoint()
	if err != nil && !errors.Is(err, isr.ErrEmptyState) {
		return err
	}
	if err == nil {
		clearSnapshot := checkpoint.LogStartOffset > seq
		if checkpoint.HW > seq {
			checkpoint.HW = seq
		}
		if checkpoint.LogStartOffset > seq {
			checkpoint.LogStartOffset = seq
		}
		if err := s.storeCheckpointAndMaybeDeleteSnapshot(checkpoint, clearSnapshot); err != nil {
			return err
		}
	}
	return s.trimEpochHistoryAfter(seq + 1)
}

func (s *Store) committedHW() (uint64, error) {
	checkpoint, err := s.loadCheckpoint()
	if errors.Is(err, isr.ErrEmptyState) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return checkpoint.HW, nil
}

func decodeChannelMessage(record LogRecord) (ChannelMessage, error) {
	message, err := decodeStoredMessageView(record.Payload)
	if err != nil {
		return ChannelMessage{}, err
	}
	return ChannelMessage{
		MessageID:   message.MessageID,
		MessageSeq:  record.Offset + 1,
		SenderUID:   message.SenderUID,
		ClientMsgNo: message.ClientMsgNo,
		Payload:     message.Payload,
	}, nil
}

func (s *Store) loadRangeMsgs(startSeq, endSeq uint64, limit int) ([]ChannelMessage, error) {
	if startSeq > endSeq {
		return nil, nil
	}

	msgs := make([]ChannelMessage, 0, initialRangeMsgCapacity(limit))
	nextSeq := startSeq
	remaining := limit

	for nextSeq <= endSeq {
		batchLimit := nextSeqReadBatchLimit(nextSeq, endSeq, remaining)
		records, err := s.readOffsets(nextSeq-1, batchLimit, math.MaxInt)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			return msgs, nil
		}

		for _, record := range records {
			seq := record.Offset + 1
			if seq > endSeq {
				return msgs, nil
			}
			msg, err := decodeChannelMessage(record)
			if err != nil {
				return nil, err
			}
			msgs = append(msgs, msg)
			nextSeq = seq + 1
			if remaining > 0 {
				remaining--
				if remaining == 0 {
					return msgs, nil
				}
			}
		}

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
	if batchLimit <= 0 {
		return 1
	}
	return batchLimit
}

func initialRangeMsgCapacity(limit int) int {
	if limit <= 0 {
		return 0
	}
	return minInt(limit, seqReadChunkLimit)
}
