package log

import (
	"bytes"
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
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
	return s.appendPayloadsWithCommit(payloads, pebble.Sync, true)
}

func (s *Store) appendPayloadsNoSync(payloads [][]byte) (uint64, error) {
	return s.appendPayloadsWithCommit(payloads, pebble.NoSync, false)
}

func (s *Store) appendPayloadsWithCommit(payloads [][]byte, commitOpts *pebble.WriteOptions, recordCommit bool) (uint64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	s.mu.Lock()
	base, err := s.leoLocked()
	if err != nil {
		s.mu.Unlock()
		return 0, err
	}
	if len(payloads) == 0 {
		s.mu.Unlock()
		return base, nil
	}

	s.writeInProgress.Store(true)
	s.mu.Unlock()
	defer s.writeInProgress.Store(false)

	batch := s.db.db.NewBatch()
	defer batch.Close()

	for i, payload := range payloads {
		key := encodeLogRecordKey(s.groupKey, base+uint64(i))
		value := append([]byte(nil), payload...)
		if err := batch.Set(key, value, pebble.NoSync); err != nil {
			return 0, err
		}
	}
	if err := batch.Commit(commitOpts); err != nil {
		return 0, err
	}
	if recordCommit {
		s.recordDurableCommit()
	}
	s.mu.Lock()
	s.cachedLEO = base + uint64(len(payloads))
	s.leoLoaded = true
	s.mu.Unlock()
	return base, nil
}

func (s *Store) applyFetchedRecords(records []isr.Record, committed []appliedMessage, checkpoint *isr.Checkpoint) (uint64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	s.mu.Lock()
	base, err := s.leoLocked()
	if err != nil {
		s.mu.Unlock()
		return 0, err
	}
	if len(records) == 0 && checkpoint == nil {
		s.mu.Unlock()
		return base, nil
	}
	s.writeInProgress.Store(true)
	s.mu.Unlock()
	defer s.writeInProgress.Store(false)

	nextLEO := base + uint64(len(records))
	if coordinator := s.commitCoordinator(); coordinator != nil {
		err := coordinator.submit(commitRequest{
			groupKey: s.groupKey,
			build: func(writeBatch *pebble.Batch) error {
				return s.writeApplyFetchedRecords(writeBatch, base, records, committed, checkpoint)
			},
			publish: func() error {
				s.recordDurableCommit()
				s.mu.Lock()
				s.cachedLEO = nextLEO
				s.leoLoaded = true
				s.mu.Unlock()
				return nil
			},
		})
		if err != nil {
			return 0, err
		}
		return nextLEO, nil
	}

	batch := s.db.db.NewBatch()
	defer batch.Close()

	if err := s.writeApplyFetchedRecords(batch, base, records, committed, checkpoint); err != nil {
		return 0, err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return 0, err
	}
	s.recordDurableCommit()
	s.mu.Lock()
	s.cachedLEO = nextLEO
	s.leoLoaded = true
	s.mu.Unlock()
	return s.cachedLEO, nil
}

func (s *Store) writeApplyFetchedRecords(writeBatch *pebble.Batch, base uint64, records []isr.Record, committed []appliedMessage, checkpoint *isr.Checkpoint) error {
	for i, record := range records {
		key := encodeLogRecordKey(s.groupKey, base+uint64(i))
		value := append([]byte(nil), record.Payload...)
		if err := writeBatch.Set(key, value, pebble.NoSync); err != nil {
			return err
		}
	}
	for _, msg := range committed {
		if err := writeBatch.Set(
			encodeIdempotencyKey(s.groupKey, msg.key),
			encodeIdempotencyEntry(msg.entry),
			pebble.NoSync,
		); err != nil {
			return err
		}
	}
	if checkpoint != nil {
		if err := s.writeCheckpoint(writeBatch, *checkpoint); err != nil {
			return err
		}
	}
	return nil
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
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leoLocked()
}

func (s *Store) leoLocked() (uint64, error) {
	if s.leoLoaded {
		return s.cachedLEO, nil
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
		s.cachedLEO = 0
		s.leoLoaded = true
		return 0, nil
	}
	offset, err := decodeLogRecordOffset(iter.Key(), prefix)
	if err != nil {
		return 0, err
	}
	s.cachedLEO = offset + 1
	s.leoLoaded = true
	return s.cachedLEO, nil
}

func (s *Store) truncateOffsets(to uint64) error {
	if err := s.validate(); err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	s.mu.Lock()
	leo, err := s.leoLocked()
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if to >= leo {
		s.mu.Unlock()
		return nil
	}
	s.writeInProgress.Store(true)
	s.mu.Unlock()
	defer s.writeInProgress.Store(false)

	prefix := encodeLogPrefix(s.groupKey)
	batch := s.db.db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange(encodeLogRecordKey(s.groupKey, to), keyUpperBound(prefix), pebble.NoSync); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	s.recordDurableCommit()
	s.mu.Lock()
	s.cachedLEO = to
	s.leoLoaded = true
	s.mu.Unlock()
	return nil
}

func (s *Store) sync() error {
	if err := s.validate(); err != nil {
		return err
	}
	return nil
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
