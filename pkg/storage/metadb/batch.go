package metadb

import "github.com/cockroachdb/pebble/v2"

// WriteBatch accumulates multiple writes into a single pebble batch,
// committing them atomically with one fsync in Commit.
type WriteBatch struct {
	db              *DB
	batch           *pebble.Batch
	writtenUserKeys map[string]struct{}
}

// NewWriteBatch creates a new WriteBatch. The caller must call Close
// when done, even if Commit is not called.
func (db *DB) NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		db:    db,
		batch: db.db.NewBatch(),
	}
}

// CreateUser encodes and stages a create-only user write into the batch.
// If the user already exists in the database or earlier in the same indexed
// batch, the existing record is preserved and the operation becomes a no-op.
func (b *WriteBatch) CreateUser(slot uint64, u User) error {
	if err := validateSlot(slot); err != nil {
		return err
	}
	if err := validateUser(u); err != nil {
		return err
	}

	key := encodeUserPrimaryKey(slot, u.UID, userPrimaryFamilyID)
	if b.userKeyWritten(key) {
		return nil
	}
	exists, err := b.db.hasKey(key)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	value := encodeUserFamilyValue(u.Token, u.DeviceFlag, u.DeviceLevel, key)
	if err := b.batch.Set(key, value, nil); err != nil {
		return err
	}
	b.markUserKeyWritten(key)
	return nil
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
	if err := b.batch.Set(key, value, nil); err != nil {
		return err
	}
	b.markUserKeyWritten(key)
	return nil
}

// UpsertDevice encodes and stages a device write into the batch.
// No lock is held; the batch is assumed single-threaded.
func (b *WriteBatch) UpsertDevice(slot uint64, d Device) error {
	if err := validateSlot(slot); err != nil {
		return err
	}
	if err := validateDevice(d); err != nil {
		return err
	}

	key := encodeDevicePrimaryKey(slot, d.UID, d.DeviceFlag, devicePrimaryFamilyID)
	value := encodeDeviceFamilyValue(d.Token, d.DeviceLevel, key)
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

// DeleteChannel removes the primary record and ID index for a channel.
func (b *WriteBatch) DeleteChannel(slot uint64, channelID string, channelType int64) error {
	primaryKey := encodeChannelPrimaryKey(slot, channelID, channelType, channelPrimaryFamilyID)
	if err := b.batch.Delete(primaryKey, nil); err != nil {
		return err
	}
	indexKey := encodeChannelIDIndexKey(slot, channelID, channelType)
	return b.batch.Delete(indexKey, nil)
}

// UpsertChannelRuntimeMeta encodes and stages a runtime metadata write into the batch.
func (b *WriteBatch) UpsertChannelRuntimeMeta(slot uint64, meta ChannelRuntimeMeta) error {
	if err := validateSlot(slot); err != nil {
		return err
	}
	if err := validateChannelRuntimeMeta(meta); err != nil {
		return err
	}

	meta = normalizeChannelRuntimeMeta(meta)

	key := encodeChannelRuntimeMetaPrimaryKey(slot, meta.ChannelID, meta.ChannelType, channelRuntimeMetaPrimaryFamilyID)
	value := encodeChannelRuntimeMetaFamilyValue(meta, key)
	return b.batch.Set(key, value, nil)
}

// DeleteChannelRuntimeMeta removes the runtime metadata record for a channel.
func (b *WriteBatch) DeleteChannelRuntimeMeta(slot uint64, channelID string, channelType int64) error {
	if err := validateSlot(slot); err != nil {
		return err
	}
	if err := validateChannelRuntimeMetaChannelID(channelID); err != nil {
		return err
	}
	key := encodeChannelRuntimeMetaPrimaryKey(slot, channelID, channelType, channelRuntimeMetaPrimaryFamilyID)
	return b.batch.Delete(key, nil)
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

func (b *WriteBatch) markUserKeyWritten(key []byte) {
	if b.writtenUserKeys == nil {
		b.writtenUserKeys = make(map[string]struct{}, 1)
	}
	b.writtenUserKeys[string(key)] = struct{}{}
}

func (b *WriteBatch) userKeyWritten(key []byte) bool {
	if b.writtenUserKeys == nil {
		return false
	}
	_, ok := b.writtenUserKeys[string(key)]
	return ok
}
