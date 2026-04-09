package channellog

import (
	"sync"

	"github.com/cockroachdb/pebble/v2"
)

type DB struct {
	mu          sync.Mutex
	db          *pebble.DB
	stores      map[ChannelKey]*Store
	coordinator *commitCoordinator
}

func Open(path string) (*DB, error) {
	pdb, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &DB{
		db:     pdb,
		stores: make(map[ChannelKey]*Store),
	}, nil
}

func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	db.mu.Lock()
	if db.db == nil {
		db.mu.Unlock()
		return nil
	}
	pdb := db.db
	coordinator := db.coordinator
	db.db = nil
	db.stores = nil
	db.coordinator = nil
	db.mu.Unlock()
	if coordinator != nil {
		coordinator.close()
	}
	return pdb.Close()
}

func (db *DB) ForChannel(key ChannelKey) *Store {
	if db == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.stores == nil {
		db.stores = make(map[ChannelKey]*Store)
	}
	if store, ok := db.stores[key]; ok {
		return store
	}
	store := &Store{
		db:       db,
		key:      key,
		groupKey: channelGroupKey(key),
	}
	db.stores[key] = store
	return store
}

func (db *DB) StateStoreFactory() StateStoreFactory {
	return &stateStoreFactory{db: db}
}

func (db *DB) commitCoordinator() *commitCoordinator {
	if db == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.db == nil {
		return nil
	}
	if db.coordinator == nil {
		db.coordinator = newCommitCoordinator(db.db)
	}
	return db.coordinator
}
