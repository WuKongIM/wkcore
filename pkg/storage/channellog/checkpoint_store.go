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
	return s.db.db.Set(encodeCheckpointKey(s.groupKey), encodeCheckpoint(checkpoint), pebble.Sync)
}
