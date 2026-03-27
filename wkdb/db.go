package wkdb

import (
	"context"
	"errors"
	"sync"

	"github.com/cockroachdb/pebble"
)

type DB struct {
	db *pebble.DB
	mu sync.RWMutex

	testHooks dbTestHooks
}

type dbTestHooks struct {
	afterExistenceCheck func()
	beforeImportCommit  func() error
}

func Open(path string) (*DB, error) {
	pdb, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &DB{db: pdb}, nil
}

func (db *DB) Close() error {
	if db == nil || db.db == nil {
		return nil
	}
	return db.db.Close()
}

func (db *DB) ForSlot(slot uint64) *ShardStore {
	return &ShardStore{db: db, slot: slot}
}

func (db *DB) getValue(key []byte) ([]byte, error) {
	value, closer, err := db.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer closer.Close()

	return append([]byte(nil), value...), nil
}

func (db *DB) hasKey(key []byte) (bool, error) {
	_, closer, err := db.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	defer closer.Close()
	return true, nil
}

func (db *DB) checkContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func (db *DB) runAfterExistenceCheckHook() {
	if db.testHooks.afterExistenceCheck != nil {
		db.testHooks.afterExistenceCheck()
	}
}

func (db *DB) DeleteSlotData(ctx context.Context, slotID uint64) error {
	if err := validateSlot(slotID); err != nil {
		return err
	}
	if err := db.checkContext(ctx); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	batch := db.db.NewBatch()
	defer batch.Close()

	for _, span := range slotAllDataSpans(slotID) {
		if err := batch.DeleteRange(span.Start, span.End, nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}
