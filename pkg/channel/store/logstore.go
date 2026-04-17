package store

import (
	"bytes"
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/cockroachdb/pebble/v2"
)

const logScanInitialCapacity = 16

type LogRecord struct {
	Offset  uint64
	Payload []byte
}

type pendingLogAppend struct {
	base    uint64
	nextLEO uint64
	entries []logBatchEntry
}

type logBatchEntry struct {
	key   []byte
	value []byte
}

func (s *ChannelStore) validate() error {
	if s == nil || s.engine == nil || s.engine.db == nil || s.key == "" {
		return channel.ErrInvalidArgument
	}
	return nil
}

func (s *ChannelStore) Append(records []channel.Record) (uint64, error) {
	return s.appendRecordsWithCommit(records, pebble.Sync, true)
}

func (s *ChannelStore) appendRecordsNoSync(records []channel.Record) (uint64, error) {
	return s.appendRecordsWithCommit(records, pebble.NoSync, false)
}

func (s *ChannelStore) appendRecordsWithCommit(records []channel.Record, commitOpts *pebble.WriteOptions, recordCommit bool) (uint64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	pending, err := s.prepareAppendLocked(records)
	if err != nil {
		return 0, err
	}
	if len(pending.entries) == 0 {
		return pending.base, nil
	}

	if commitOpts == pebble.Sync && recordCommit {
		if err := s.appendWithCoordinatorLocked(pending); err != nil {
			s.failPendingWrite()
			return 0, err
		}
		return pending.base, nil
	}

	batch := s.engine.db.NewBatch()
	defer batch.Close()

	if err := pending.build(batch); err != nil {
		s.failPendingWrite()
		return 0, err
	}
	if err := batch.Commit(commitOpts); err != nil {
		s.failPendingWrite()
		return 0, err
	}
	if recordCommit {
		s.publishDurableWrite(pending.nextLEO)
		return pending.base, nil
	}
	s.publishWrite(pending.nextLEO)
	return pending.base, nil
}

func (s *ChannelStore) prepareAppendLocked(records []channel.Record) (pendingLogAppend, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	base, err := s.leoLocked()
	if err != nil {
		return pendingLogAppend{}, err
	}
	if len(records) == 0 {
		return pendingLogAppend{base: base}, nil
	}

	s.writeInProgress.Store(true)
	pending := pendingLogAppend{
		base:    base,
		nextLEO: base + uint64(len(records)),
		entries: make([]logBatchEntry, 0, len(records)),
	}
	for i, record := range records {
		pending.entries = append(pending.entries, logBatchEntry{
			key:   encodeLogRecordKey(s.key, base+uint64(i)),
			value: append([]byte(nil), record.Payload...),
		})
	}
	return pending, nil
}

func (s *ChannelStore) appendWithCoordinatorLocked(pending pendingLogAppend) error {
	coordinator := s.commitCoordinator()
	if coordinator == nil {
		return channel.ErrInvalidArgument
	}
	return coordinator.submit(commitRequest{
		channelKey: s.key,
		build: func(writeBatch *pebble.Batch) error {
			return pending.build(writeBatch)
		},
		publish: func() error {
			s.publishDurableWrite(pending.nextLEO)
			return nil
		},
	})
}

func (p pendingLogAppend) build(writeBatch *pebble.Batch) error {
	for _, entry := range p.entries {
		if err := writeBatch.Set(entry.key, entry.value, pebble.NoSync); err != nil {
			return err
		}
	}
	return nil
}

func (s *ChannelStore) Read(from uint64, maxBytes int) ([]channel.Record, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if maxBytes <= 0 {
		return nil, nil
	}

	records := make([]channel.Record, 0, logScanInitialCapacity)
	_, err := s.engine.scanOffsets(s.key, from, maxLogScanLimit(), maxBytes, func(_ uint64, payload []byte) error {
		records = append(records, channel.Record{Payload: payload, SizeBytes: len(payload)})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (s *ChannelStore) ReadOffsets(fromOffset uint64, limit int, maxBytes int) ([]LogRecord, error) {
	return s.readOffsets(fromOffset, limit, maxBytes)
}

func (s *ChannelStore) readOffsets(fromOffset uint64, limit int, maxBytes int) ([]LogRecord, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	return s.engine.readOffsets(s.key, fromOffset, limit, maxBytes)
}

func (s *ChannelStore) LEO() uint64 {
	if s == nil {
		return 0
	}
	if s.writeInProgress.Load() {
		return s.leo.Load()
	}
	leo, err := s.leoWithError()
	if err != nil {
		return s.leo.Load()
	}
	return leo
}

func (s *ChannelStore) leoWithError() (uint64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leoLocked()
}

func (s *ChannelStore) leoLocked() (uint64, error) {
	if s.loaded.Load() {
		return s.leo.Load(), nil
	}

	prefix := encodeLogPrefix(s.key)
	iter, err := s.engine.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: keyUpperBound(prefix)})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	if !iter.Last() {
		s.leo.Store(0)
		s.loaded.Store(true)
		return 0, nil
	}
	offset, err := decodeLogRecordOffset(iter.Key(), prefix)
	if err != nil {
		return 0, err
	}
	next := offset + 1
	s.leo.Store(next)
	s.loaded.Store(true)
	return next, nil
}

func (s *ChannelStore) Truncate(to uint64) error {
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

	prefix := encodeLogPrefix(s.key)
	batch := s.engine.db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange(encodeLogRecordKey(s.key, to), keyUpperBound(prefix), pebble.NoSync); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	s.recordDurableCommit()
	s.mu.Lock()
	s.leo.Store(to)
	s.loaded.Store(true)
	s.mu.Unlock()
	return nil
}

func (s *ChannelStore) Sync() error {
	if err := s.validate(); err != nil {
		return err
	}
	return nil
}

func (e *Engine) Read(channelKey channel.ChannelKey, fromOffset uint64, limit int, maxBytes int) ([]LogRecord, error) {
	if e == nil || e.db == nil || channelKey == "" {
		return nil, channel.ErrInvalidArgument
	}
	return e.readOffsets(channelKey, fromOffset, limit, maxBytes)
}

func (e *Engine) readOffsets(channelKey channel.ChannelKey, fromOffset uint64, limit int, maxBytes int) ([]LogRecord, error) {
	if limit <= 0 || maxBytes <= 0 {
		return nil, nil
	}

	out := make([]LogRecord, 0, minInt(limit, logScanInitialCapacity))
	_, err := e.scanOffsets(channelKey, fromOffset, limit, maxBytes, func(offset uint64, payload []byte) error {
		out = append(out, LogRecord{Offset: offset, Payload: payload})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (e *Engine) scanOffsets(channelKey channel.ChannelKey, fromOffset uint64, limit int, maxBytes int, visit func(offset uint64, payload []byte) error) (int, error) {
	if limit <= 0 || maxBytes <= 0 {
		return 0, nil
	}

	prefix := encodeLogPrefix(channelKey)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: encodeLogRecordKey(channelKey, fromOffset),
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
		size := len(payload)
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
