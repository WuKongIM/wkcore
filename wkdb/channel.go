package wkdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

type Channel struct {
	ChannelID   string
	ChannelType int64
	Ban         int64
}

func (s *ShardStore) CreateChannel(ctx context.Context, ch Channel) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := s.db.checkContext(ctx); err != nil {
		return err
	}
	if err := validateChannel(ch); err != nil {
		return err
	}

	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	primaryKey := encodeChannelPrimaryKey(s.slot, ch.ChannelID, ch.ChannelType, channelPrimaryFamilyID)
	if _, err := s.db.getValue(primaryKey); err == nil {
		return ErrAlreadyExists
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	s.db.runAfterExistenceCheckHook()
	if err := s.db.checkContext(ctx); err != nil {
		return err
	}

	value := encodeChannelFamilyValue(ch.Ban, primaryKey)
	indexKey := encodeChannelIDIndexKey(s.slot, ch.ChannelID, ch.ChannelType)

	batch := s.db.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(primaryKey, value, nil); err != nil {
		return err
	}
	if err := batch.Set(indexKey, []byte{}, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (s *ShardStore) GetChannel(ctx context.Context, channelID string, channelType int64) (Channel, error) {
	if err := s.validate(); err != nil {
		return Channel{}, err
	}
	if err := s.db.checkContext(ctx); err != nil {
		return Channel{}, err
	}
	if channelID == "" {
		return Channel{}, ErrInvalidArgument
	}

	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	return s.getChannelLocked(channelID, channelType)
}

func (s *ShardStore) getChannelLocked(channelID string, channelType int64) (Channel, error) {
	primaryKey := encodeChannelPrimaryKey(s.slot, channelID, channelType, channelPrimaryFamilyID)
	value, err := s.db.getValue(primaryKey)
	if err != nil {
		return Channel{}, err
	}

	ban, err := decodeChannelFamilyValue(primaryKey, value)
	if err != nil {
		return Channel{}, err
	}

	return Channel{
		ChannelID:   channelID,
		ChannelType: channelType,
		Ban:         ban,
	}, nil
}

func (s *ShardStore) ListChannelsByChannelID(ctx context.Context, channelID string) ([]Channel, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if err := s.db.checkContext(ctx); err != nil {
		return nil, err
	}
	if channelID == "" {
		return nil, ErrInvalidArgument
	}

	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	prefix := encodeChannelIDIndexPrefix(s.slot, channelID)
	iter, err := s.db.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var channels []Channel
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		if err := s.db.checkContext(ctx); err != nil {
			return nil, err
		}
		key := iter.Key()
		if !bytes.HasPrefix(key, prefix) {
			break
		}

		channelType, rest, err := decodeOrderedInt64(key[len(prefix):])
		if err != nil {
			return nil, err
		}
		if len(rest) != 0 {
			return nil, fmt.Errorf("%w: malformed channel index key", ErrCorruptValue)
		}

		ch, err := s.getChannelLocked(channelID, channelType)
		if err != nil {
			return nil, err
		}
		channels = append(channels, ch)
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}
	return channels, nil
}

func (s *ShardStore) UpdateChannel(ctx context.Context, ch Channel) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := s.db.checkContext(ctx); err != nil {
		return err
	}
	if err := validateChannel(ch); err != nil {
		return err
	}

	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	primaryKey := encodeChannelPrimaryKey(s.slot, ch.ChannelID, ch.ChannelType, channelPrimaryFamilyID)
	if _, err := s.db.getValue(primaryKey); err != nil {
		return err
	}
	if err := s.db.checkContext(ctx); err != nil {
		return err
	}

	value := encodeChannelFamilyValue(ch.Ban, primaryKey)

	batch := s.db.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(primaryKey, value, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (s *ShardStore) DeleteChannel(ctx context.Context, channelID string, channelType int64) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := s.db.checkContext(ctx); err != nil {
		return err
	}
	if channelID == "" {
		return ErrInvalidArgument
	}

	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	primaryKey := encodeChannelPrimaryKey(s.slot, channelID, channelType, channelPrimaryFamilyID)
	if _, err := s.db.getValue(primaryKey); err != nil {
		return err
	}
	if err := s.db.checkContext(ctx); err != nil {
		return err
	}

	indexKey := encodeChannelIDIndexKey(s.slot, channelID, channelType)

	batch := s.db.db.NewBatch()
	defer batch.Close()

	if err := batch.Delete(primaryKey, nil); err != nil {
		return err
	}
	if err := batch.Delete(indexKey, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func validateChannel(ch Channel) error {
	if ch.ChannelID == "" || len(ch.ChannelID) > maxKeyStringLen {
		return ErrInvalidArgument
	}
	return nil
}
