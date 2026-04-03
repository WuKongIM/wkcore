package channellog

import (
	"errors"

	"github.com/cockroachdb/pebble/v2"
)

func (s *Store) loadSnapshotPayload() ([]byte, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	value, closer, err := s.db.db.Get(encodeSnapshotKey(s.groupKey))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	defer closer.Close()
	return append([]byte(nil), value...), nil
}

func (s *Store) storeSnapshotPayload(payload []byte) error {
	if err := s.validate(); err != nil {
		return err
	}
	return s.db.db.Set(encodeSnapshotKey(s.groupKey), append([]byte(nil), payload...), pebble.Sync)
}
