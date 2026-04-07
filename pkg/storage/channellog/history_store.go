package channellog

import (
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
	"github.com/cockroachdb/pebble/v2"
)

func (s *Store) loadEpochHistory() ([]isr.EpochPoint, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	prefix := encodeHistoryPrefix(s.groupKey)
	iter, err := s.db.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: keyUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	points := make([]isr.EpochPoint, 0)
	for valid := iter.First(); valid; valid = iter.Next() {
		point, err := decodeEpochPoint(iter.Value())
		if err != nil {
			return nil, err
		}
		points = append(points, point)
	}
	if len(points) == 0 {
		return nil, isr.ErrEmptyState
	}
	return points, nil
}

func (s *Store) appendEpochPoint(point isr.EpochPoint) error {
	if err := s.validate(); err != nil {
		return err
	}
	return s.db.db.Set(encodeHistoryKey(s.groupKey, point.StartOffset), encodeEpochPoint(point), pebble.Sync)
}

func (s *Store) truncateEpochHistoryTo(leo uint64) error {
	if leo == ^uint64(0) {
		return nil
	}
	return s.trimEpochHistoryAfter(leo + 1)
}

func (s *Store) trimEpochHistoryAfter(startOffset uint64) error {
	if err := s.validate(); err != nil {
		return err
	}
	prefix := encodeHistoryPrefix(s.groupKey)
	batch := s.db.db.NewBatch()
	defer batch.Close()
	if err := batch.DeleteRange(encodeHistoryKey(s.groupKey, startOffset), keyUpperBound(prefix), pebble.Sync); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (s *Store) loadEpochHistoryOrEmpty() ([]isr.EpochPoint, error) {
	points, err := s.loadEpochHistory()
	if errors.Is(err, isr.ErrEmptyState) {
		return nil, nil
	}
	return points, err
}
