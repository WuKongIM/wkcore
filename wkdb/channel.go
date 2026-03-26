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

func (db *DB) CreateChannel(ctx context.Context, ch Channel) error {
	if err := db.checkContext(ctx); err != nil {
		return err
	}
	if err := validateChannel(ch); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	primaryKey := encodeChannelPrimaryKey(ch.ChannelID, ch.ChannelType, channelPrimaryFamilyID)
	if _, err := db.getValue(primaryKey); err == nil {
		return ErrAlreadyExists
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	db.runAfterExistenceCheckHook()
	if err := db.checkContext(ctx); err != nil {
		return err
	}

	value := encodeChannelFamilyValue(ch.Ban, primaryKey)
	indexKey := encodeChannelIDIndexKey(ch.ChannelID, ch.ChannelType)

	batch := db.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(primaryKey, value, nil); err != nil {
		return err
	}
	if err := batch.Set(indexKey, []byte{}, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (db *DB) GetChannel(ctx context.Context, channelID string, channelType int64) (Channel, error) {
	if err := db.checkContext(ctx); err != nil {
		return Channel{}, err
	}
	if channelID == "" {
		return Channel{}, ErrInvalidArgument
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.getChannelLocked(channelID, channelType)
}

func (db *DB) getChannelLocked(channelID string, channelType int64) (Channel, error) {
	primaryKey := encodeChannelPrimaryKey(channelID, channelType, channelPrimaryFamilyID)
	value, err := db.getValue(primaryKey)
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

func (db *DB) ListChannelsByChannelID(ctx context.Context, channelID string) ([]Channel, error) {
	if err := db.checkContext(ctx); err != nil {
		return nil, err
	}
	if channelID == "" {
		return nil, ErrInvalidArgument
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	prefix := encodeChannelIDIndexPrefix(channelID)
	iter, err := db.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var channels []Channel
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		if err := db.checkContext(ctx); err != nil {
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

		ch, err := db.getChannelLocked(channelID, channelType)
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

func (db *DB) UpdateChannel(ctx context.Context, ch Channel) error {
	if err := db.checkContext(ctx); err != nil {
		return err
	}
	if err := validateChannel(ch); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	primaryKey := encodeChannelPrimaryKey(ch.ChannelID, ch.ChannelType, channelPrimaryFamilyID)
	if _, err := db.getValue(primaryKey); err != nil {
		return err
	}
	if err := db.checkContext(ctx); err != nil {
		return err
	}

	value := encodeChannelFamilyValue(ch.Ban, primaryKey)

	batch := db.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(primaryKey, value, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (db *DB) DeleteChannel(ctx context.Context, channelID string, channelType int64) error {
	if err := db.checkContext(ctx); err != nil {
		return err
	}
	if channelID == "" {
		return ErrInvalidArgument
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	primaryKey := encodeChannelPrimaryKey(channelID, channelType, channelPrimaryFamilyID)
	if _, err := db.getValue(primaryKey); err != nil {
		return err
	}
	if err := db.checkContext(ctx); err != nil {
		return err
	}

	indexKey := encodeChannelIDIndexKey(channelID, channelType)

	batch := db.db.NewBatch()
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
