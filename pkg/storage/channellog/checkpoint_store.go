package channellog

import (
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
	"github.com/cockroachdb/pebble/v2"
)

func (s *Store) loadCheckpoint() (isr.Checkpoint, error) {
	if err := s.validate(); err != nil {
		return isr.Checkpoint{}, err
	}
	value, closer, err := s.db.db.Get(encodeCheckpointKey(s.groupKey))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return isr.Checkpoint{}, isr.ErrEmptyState
		}
		return isr.Checkpoint{}, err
	}
	defer closer.Close()
	return decodeCheckpoint(value)
}

func (s *Store) storeCheckpoint(checkpoint isr.Checkpoint) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := s.db.db.Set(encodeCheckpointKey(s.groupKey), encodeCheckpoint(checkpoint), pebble.Sync); err != nil {
		return err
	}
	s.recordDurableCommit()
	return nil
}

func (s *Store) storeCheckpointAndMaybeDeleteSnapshot(checkpoint isr.Checkpoint, deleteSnapshot bool) error {
	if err := s.validate(); err != nil {
		return err
	}

	batch := s.db.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(encodeCheckpointKey(s.groupKey), encodeCheckpoint(checkpoint), pebble.NoSync); err != nil {
		return err
	}
	if deleteSnapshot {
		if err := batch.Delete(encodeSnapshotKey(s.groupKey), pebble.NoSync); err != nil {
			return err
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	s.recordDurableCommit()
	return nil
}
