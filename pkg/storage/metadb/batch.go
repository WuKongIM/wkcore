package metadb

import "github.com/cockroachdb/pebble/v2"

// WriteBatch accumulates multiple writes into a single pebble batch,
// committing them atomically with one fsync in Commit.
type WriteBatch struct {
	db                     *DB
	batch                  *pebble.Batch
	writtenUserKeys        map[string]struct{}
	userConversationStates map[string]userConversationStateBatchEntry
}

type userConversationStateBatchEntry struct {
	state  UserConversationState
	exists bool
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
	if err := b.batch.Delete(indexKey, nil); err != nil {
		return err
	}
	subscriberPrefix := encodeSubscriberChannelPrefix(slot, channelID, channelType)
	return b.batch.DeleteRange(subscriberPrefix, nextPrefix(subscriberPrefix), nil)
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

// UpsertUserConversationState encodes and stages a user conversation state write.
func (b *WriteBatch) UpsertUserConversationState(slot uint64, state UserConversationState) error {
	if err := validateSlot(slot); err != nil {
		return err
	}
	if err := validateUserConversationState(state); err != nil {
		return err
	}

	primaryKey := encodeUserConversationStatePrimaryKey(slot, state.UID, state.ChannelType, state.ChannelID, userConversationStatePrimaryFamilyID)
	existing, exists, err := b.loadUserConversationState(slot, primaryKey, state.UID, state.ChannelID, state.ChannelType)
	if err != nil {
		return err
	}
	if exists && state.ActiveAt < existing.ActiveAt {
		state.ActiveAt = existing.ActiveAt
	}
	value := encodeUserConversationStateFamilyValue(state, primaryKey)
	if exists && existing.ActiveAt > 0 && existing.ActiveAt != state.ActiveAt {
		oldIndexKey := encodeUserConversationActiveIndexKey(slot, state.UID, existing.ActiveAt, state.ChannelType, state.ChannelID)
		if err := b.batch.Delete(oldIndexKey, nil); err != nil {
			return err
		}
	}
	if err := b.batch.Set(primaryKey, value, nil); err != nil {
		return err
	}
	if state.ActiveAt > 0 {
		indexKey := encodeUserConversationActiveIndexKey(slot, state.UID, state.ActiveAt, state.ChannelType, state.ChannelID)
		if err := b.batch.Set(indexKey, []byte{}, nil); err != nil {
			return err
		}
	}
	b.rememberUserConversationState(primaryKey, state, true)
	return nil
}

// UpsertChannelUpdateLog encodes and stages a channel update log write.
func (b *WriteBatch) UpsertChannelUpdateLog(slot uint64, entry ChannelUpdateLog) error {
	if err := validateSlot(slot); err != nil {
		return err
	}
	if err := validateChannelUpdateLog(entry); err != nil {
		return err
	}

	key := encodeChannelUpdateLogPrimaryKey(slot, entry.ChannelID, entry.ChannelType, channelUpdateLogPrimaryFamilyID)
	value := encodeChannelUpdateLogFamilyValue(entry, key)
	return b.batch.Set(key, value, nil)
}

// DeleteChannelUpdateLogs removes channel update log rows for the provided keys.
func (b *WriteBatch) DeleteChannelUpdateLogs(slot uint64, keys []ConversationKey) error {
	if err := validateSlot(slot); err != nil {
		return err
	}

	normalized, err := normalizeConversationKeys(keys)
	if err != nil {
		return err
	}
	for _, key := range normalized {
		primaryKey := encodeChannelUpdateLogPrimaryKey(slot, key.ChannelID, key.ChannelType, channelUpdateLogPrimaryFamilyID)
		if err := b.batch.Delete(primaryKey, nil); err != nil {
			return err
		}
	}
	return nil
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

// AddSubscribers stages subscriber snapshot rows into the batch.
func (b *WriteBatch) AddSubscribers(slot uint64, channelID string, channelType int64, uids []string) error {
	if err := validateSlot(slot); err != nil {
		return err
	}
	if err := validateSubscriberChannel(channelID); err != nil {
		return err
	}

	normalized, err := normalizeSubscriberUIDs(uids)
	if err != nil {
		return err
	}
	for _, uid := range normalized {
		key := encodeSubscriberPrimaryKey(slot, channelID, channelType, uid, subscriberPrimaryFamilyID)
		if err := b.batch.Set(key, wrapFamilyValue(key, nil), nil); err != nil {
			return err
		}
	}
	return nil
}

// RemoveSubscribers stages subscriber snapshot row deletions into the batch.
func (b *WriteBatch) RemoveSubscribers(slot uint64, channelID string, channelType int64, uids []string) error {
	if err := validateSlot(slot); err != nil {
		return err
	}
	if err := validateSubscriberChannel(channelID); err != nil {
		return err
	}

	normalized, err := normalizeSubscriberUIDs(uids)
	if err != nil {
		return err
	}
	for _, uid := range normalized {
		key := encodeSubscriberPrimaryKey(slot, channelID, channelType, uid, subscriberPrimaryFamilyID)
		if err := b.batch.Delete(key, nil); err != nil {
			return err
		}
	}
	return nil
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

func (b *WriteBatch) loadUserConversationState(slot uint64, primaryKey []byte, uid, channelID string, channelType int64) (UserConversationState, bool, error) {
	if b.userConversationStates != nil {
		if entry, ok := b.userConversationStates[string(primaryKey)]; ok {
			return entry.state, entry.exists, nil
		}
	}

	shard := b.db.ForSlot(slot)
	state, err := shard.getUserConversationStateLocked(uid, channelID, channelType)
	switch err {
	case nil:
		b.rememberUserConversationState(primaryKey, state, true)
		return state, true, nil
	case ErrNotFound:
		b.rememberUserConversationState(primaryKey, UserConversationState{}, false)
		return UserConversationState{}, false, nil
	default:
		return UserConversationState{}, false, err
	}
}

func (b *WriteBatch) rememberUserConversationState(primaryKey []byte, state UserConversationState, exists bool) {
	if b.userConversationStates == nil {
		b.userConversationStates = make(map[string]userConversationStateBatchEntry, 1)
	}
	b.userConversationStates[string(primaryKey)] = userConversationStateBatchEntry{
		state:  state,
		exists: exists,
	}
}
