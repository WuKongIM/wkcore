package channellog

import (
	"bytes"
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
	"github.com/cockroachdb/pebble/v2"
)

const logScanInitialCapacity = 16

func (s *Store) validate() error {
	if s == nil || s.db == nil || s.db.db == nil || s.groupKey == "" {
		return ErrInvalidArgument
	}
	return nil
}

func (s *Store) appendPayloads(payloads [][]byte) (uint64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	base, err := s.leo()
	if err != nil {
		return 0, err
	}
	if len(payloads) == 0 {
		return base, nil
	}

	batch := s.db.db.NewBatch()
	defer batch.Close()

	for i, payload := range payloads {
		key := encodeLogRecordKey(s.groupKey, base+uint64(i))
		value := append([]byte(nil), payload...)
		if err := batch.Set(key, value, pebble.NoSync); err != nil {
			return 0, err
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return 0, err
	}
	return base, nil
}

func (s *Store) readOffsets(fromOffset uint64, limit int, maxBytes int) ([]LogRecord, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	return s.db.readGroupOffsets(s.groupKey, fromOffset, limit, maxBytes)
}

func (s *Store) leo() (uint64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	prefix := encodeLogPrefix(s.groupKey)
	iter, err := s.db.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: keyUpperBound(prefix),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	if !iter.Last() {
		return 0, nil
	}
	offset, err := decodeLogRecordOffset(iter.Key(), prefix)
	if err != nil {
		return 0, err
	}
	return offset + 1, nil
}

func (s *Store) truncateOffsets(to uint64) error {
	if err := s.validate(); err != nil {
		return err
	}
	leo, err := s.leo()
	if err != nil {
		return err
	}
	if to >= leo {
		return nil
	}

	prefix := encodeLogPrefix(s.groupKey)
	batch := s.db.db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange(encodeLogRecordKey(s.groupKey, to), keyUpperBound(prefix), pebble.NoSync); err != nil {
		return err
	}
	return batch.Commit(pebble.NoSync)
}

func (s *Store) sync() error {
	if err := s.validate(); err != nil {
		return err
	}
	return s.db.db.Flush()
}

func (db *DB) Read(groupKey isr.GroupKey, fromOffset uint64, limit int, maxBytes int) ([]LogRecord, error) {
	if db == nil || db.db == nil || groupKey == "" {
		return nil, ErrInvalidArgument
	}
	return db.readGroupOffsets(groupKey, fromOffset, limit, maxBytes)
}

func (db *DB) readGroupOffsets(groupKey isr.GroupKey, fromOffset uint64, limit int, maxBytes int) ([]LogRecord, error) {
	if limit <= 0 || maxBytes <= 0 {
		return nil, nil
	}

	out := make([]LogRecord, 0, minInt(limit, logScanInitialCapacity))
	_, err := db.scanGroupOffsets(groupKey, fromOffset, limit, maxBytes, func(offset uint64, payload []byte) error {
		out = append(out, LogRecord{
			Offset:  offset,
			Payload: payload,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func logRecordReadSize(payload []byte) int {
	return len(payload)
}

func (db *DB) scanGroupOffsets(groupKey isr.GroupKey, fromOffset uint64, limit int, maxBytes int, visit func(offset uint64, payload []byte) error) (int, error) {
	if limit <= 0 || maxBytes <= 0 {
		return 0, nil
	}

	prefix := encodeLogPrefix(groupKey)
	iter, err := db.db.NewIter(&pebble.IterOptions{
		LowerBound: encodeLogRecordKey(groupKey, fromOffset),
		UpperBound: keyUpperBound(prefix),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	total := 0
	count := 0
	for valid := iter.First(); valid && count < limit; valid = iter.Next() {
		if !bytes.HasPrefix(iter.Key(), prefix) {
			break
		}
		offset, err := decodeLogRecordOffset(iter.Key(), prefix)
		if err != nil {
			return count, err
		}
		payload := append([]byte(nil), iter.Value()...)
		size := logRecordReadSize(payload)
		if count > 0 && total+size > maxBytes {
			break
		}
		if err := visit(offset, payload); err != nil {
			return count, err
		}
		count++
		total += size
	}
	return count, nil
}

func maxLogScanLimit() int {
	return math.MaxInt
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
