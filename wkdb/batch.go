package wkdb

import (
	"github.com/cockroachdb/pebble/v2"
)

// WriteBatch accumulates multiple writes into a single pebble batch,
// committing them atomically with one fsync in Commit.
type WriteBatch struct {
	db    *DB
	batch *pebble.Batch
}

// NewWriteBatch creates a new WriteBatch. The caller must call Close
// when done, even if Commit is not called.
func (db *DB) NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		db:    db,
		batch: db.db.NewBatch(),
	}
}

// UpsertUser encodes and stages a user write into the batch.
// No lock is held; the batch is assumed single-threaded.
func (b *WriteBatch) UpsertUser(slot uint64, u User) error {
	if err := validateSlot(slot); err != nil {
		return err
	}
	if err := validateUser(u); err != nil {
		return err
	}

	key := encodeUserPrimaryKey(slot, u.UID, userPrimaryFamilyID)
	value := encodeUserFamilyValue(u.Token, u.DeviceFlag, u.DeviceLevel, key)
	return b.batch.Set(key, value, nil)
}

// UpsertChannel encodes and stages a channel write (primary + index)
// into the batch. No lock is held.
func (b *WriteBatch) UpsertChannel(slot uint64, ch Channel) error {
	if err := validateSlot(slot); err != nil {
		return err
	}
	if err := validateChannel(ch); err != nil {
		return err
	}

	primaryKey := encodeChannelPrimaryKey(slot, ch.ChannelID, ch.ChannelType, channelPrimaryFamilyID)
	value := encodeChannelFamilyValue(ch.Ban, primaryKey)
	indexKey := encodeChannelIDIndexKey(slot, ch.ChannelID, ch.ChannelType)
	indexValue := encodeChannelIndexValue(ch.Ban)

	if err := b.batch.Set(primaryKey, value, nil); err != nil {
		return err
	}
	return b.batch.Set(indexKey, indexValue, nil)
}

// Commit atomically writes all staged operations with a single fsync.
func (b *WriteBatch) Commit() error {
	b.db.mu.Lock()
	defer b.db.mu.Unlock()

	return b.batch.Commit(pebble.Sync)
}

// Close releases the batch resources. Safe to call after Commit.
func (b *WriteBatch) Close() {
	if b.batch != nil {
		_ = b.batch.Close()
	}
}
