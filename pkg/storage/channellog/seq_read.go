package channellog

import (
	"errors"
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

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
	if limit <= 0 {
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

	count := int(maxSeq-startSeq) + 1
	if count > limit {
		count = limit
	}
	records, err := s.readOffsets(startSeq-1, count, math.MaxInt)
	if err != nil {
		return nil, err
	}

	msgs := make([]ChannelMessage, 0, len(records))
	for _, record := range records {
		seq := record.Offset + 1
		if seq > maxSeq {
			break
		}
		msg, err := decodeChannelMessage(record)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func (s *Store) LoadPrevRangeMsgs(startSeq, endSeq uint64, limit int) ([]ChannelMessage, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if startSeq == 0 || limit <= 0 {
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

	var minSeq uint64
	maxSeq := startSeq
	if endSeq == 0 {
		if startSeq < uint64(limit) {
			minSeq = 1
		} else {
			minSeq = startSeq - uint64(limit) + 1
		}
	} else if startSeq-endSeq > uint64(limit) {
		minSeq = startSeq - uint64(limit) + 1
	} else {
		minSeq = endSeq + 1
	}

	count := int(maxSeq-minSeq) + 1
	if count > limit {
		count = limit
	}
	return s.LoadNextRangeMsgs(minSeq, maxSeq, count)
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
		if checkpoint.HW > seq {
			checkpoint.HW = seq
		}
		if checkpoint.LogStartOffset > seq {
			checkpoint.LogStartOffset = seq
		}
		if err := s.storeCheckpoint(checkpoint); err != nil {
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
	message, err := decodeStoredMessage(record.Payload)
	if err != nil {
		return ChannelMessage{}, err
	}
	return ChannelMessage{
		MessageID:   message.MessageID,
		MessageSeq:  record.Offset + 1,
		SenderUID:   message.SenderUID,
		ClientMsgNo: message.ClientMsgNo,
		Payload:     append([]byte(nil), message.Payload...),
	}, nil
}
