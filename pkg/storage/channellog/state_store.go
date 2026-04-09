package channellog

import (
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
	"github.com/cockroachdb/pebble/v2"
)

type stateStoreFactory struct {
	db *DB
}

type stateStore struct {
	store *Store
}

var _ StateStoreFactory = (*stateStoreFactory)(nil)
var _ ChannelStateStore = (*stateStore)(nil)
var _ committingStateStore = (*stateStore)(nil)
var _ batchCheckpointStateStore = (*stateStore)(nil)

func (f *stateStoreFactory) ForChannel(key ChannelKey) (ChannelStateStore, error) {
	if f == nil || f.db == nil {
		return nil, ErrInvalidArgument
	}
	store := f.db.ForChannel(key)
	if store == nil {
		return nil, ErrInvalidArgument
	}
	return &stateStore{store: store}, nil
}

func (s *stateStore) PutIdempotency(key IdempotencyKey, entry IdempotencyEntry) error {
	if err := s.validateKey(key); err != nil {
		return err
	}
	if err := s.store.db.db.Set(
		encodeIdempotencyKey(s.store.groupKey, key),
		encodeIdempotencyEntry(entry),
		pebble.Sync,
	); err != nil {
		return err
	}
	s.store.recordDurableCommit()
	return nil
}

func (s *stateStore) GetIdempotency(key IdempotencyKey) (IdempotencyEntry, bool, error) {
	if err := s.validateKey(key); err != nil {
		return IdempotencyEntry{}, false, err
	}

	value, closer, err := s.store.db.db.Get(encodeIdempotencyKey(s.store.groupKey, key))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return IdempotencyEntry{}, false, nil
		}
		return IdempotencyEntry{}, false, err
	}
	defer closer.Close()

	entry, err := decodeIdempotencyEntry(value)
	if err != nil {
		return IdempotencyEntry{}, false, err
	}
	return entry, true, nil
}

func (s *stateStore) Snapshot(offset uint64) ([]byte, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}

	prefix := encodeIdempotencyPrefix(s.store.groupKey)
	iter, err := s.store.db.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: keyUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	entries := make([]stateSnapshotEntry, 0, 16)
	for valid := iter.First(); valid; valid = iter.Next() {
		key, err := decodeIdempotencyKey(iter.Key(), prefix)
		if err != nil {
			return nil, err
		}
		entry, err := decodeIdempotencyEntry(iter.Value())
		if err != nil {
			return nil, err
		}
		if entry.Offset >= offset {
			continue
		}
		entries = append(entries, stateSnapshotEntry{
			FromUID:     key.FromUID,
			ClientMsgNo: key.ClientMsgNo,
			Entry:       entry,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return encodeStateSnapshot(entries), nil
}

func (s *stateStore) Restore(snapshot []byte) error {
	if err := s.validate(); err != nil {
		return err
	}

	entries, err := decodeStateSnapshot(snapshot)
	if err != nil {
		return err
	}

	prefix := encodeIdempotencyPrefix(s.store.groupKey)
	batch := s.store.db.db.NewBatch()
	defer batch.Close()

	if err := batch.DeleteRange(prefix, keyUpperBound(prefix), pebble.NoSync); err != nil {
		return err
	}
	for _, entry := range entries {
		key := IdempotencyKey{
			ChannelID:   s.store.key.ChannelID,
			ChannelType: s.store.key.ChannelType,
			FromUID:     entry.FromUID,
			ClientMsgNo: entry.ClientMsgNo,
		}
		if err := batch.Set(
			encodeIdempotencyKey(s.store.groupKey, key),
			encodeIdempotencyEntry(entry.Entry),
			pebble.NoSync,
		); err != nil {
			return err
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	s.store.recordDurableCommit()
	return nil
}

func (s *stateStore) CommitCommitted(checkpoint isr.Checkpoint, batch []appliedMessage) error {
	if err := s.validate(); err != nil {
		return err
	}

	writeBatch := s.store.db.db.NewBatch()
	defer writeBatch.Close()

	if err := s.writeCommitted(writeBatch, checkpoint, batch); err != nil {
		return err
	}
	if err := writeBatch.Commit(pebble.Sync); err != nil {
		return err
	}
	s.store.recordDurableCommit()
	return nil
}

func (s *stateStore) CommitCommittedWithCheckpoint(checkpoint isr.Checkpoint, batch []appliedMessage) error {
	if err := s.validate(); err != nil {
		return err
	}

	writeBatch := s.store.db.db.NewBatch()
	defer writeBatch.Close()

	if err := s.writeCommitted(writeBatch, checkpoint, batch); err != nil {
		return err
	}
	if err := writeBatch.Set(
		encodeCheckpointKey(s.store.groupKey),
		encodeCheckpoint(checkpoint),
		pebble.NoSync,
	); err != nil {
		return err
	}
	if err := writeBatch.Commit(pebble.Sync); err != nil {
		return err
	}
	s.store.recordDurableCommit()
	return nil
}

func (s *stateStore) BuildCommitCommittedWithCheckpoint(writeBatch *pebble.Batch, checkpoint isr.Checkpoint, batch []appliedMessage) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := s.writeCommitted(writeBatch, checkpoint, batch); err != nil {
		return err
	}
	return s.store.writeCheckpoint(writeBatch, checkpoint)
}

func (s *stateStore) validate() error {
	if s == nil || s.store == nil {
		return ErrInvalidArgument
	}
	return s.store.validate()
}

func (s *stateStore) validateKey(key IdempotencyKey) error {
	if err := s.validate(); err != nil {
		return err
	}
	if key.ChannelID != s.store.key.ChannelID || key.ChannelType != s.store.key.ChannelType {
		return ErrInvalidArgument
	}
	return nil
}

func (s *stateStore) writeCommitted(writeBatch *pebble.Batch, checkpoint isr.Checkpoint, batch []appliedMessage) error {
	for _, message := range batch {
		if err := s.validateKey(message.key); err != nil {
			return err
		}
		if message.entry.Offset >= checkpoint.HW {
			return ErrInvalidArgument
		}
		if err := writeBatch.Set(
			encodeIdempotencyKey(s.store.groupKey, message.key),
			encodeIdempotencyEntry(message.entry),
			pebble.NoSync,
		); err != nil {
			return err
		}
	}
	return nil
}
