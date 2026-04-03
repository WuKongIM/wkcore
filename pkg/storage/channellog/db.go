package channellog

import "github.com/cockroachdb/pebble/v2"

type DB struct {
	db *pebble.DB
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

func (db *DB) ForChannel(key ChannelKey) *Store {
	if db == nil {
		return nil
	}
	return &Store{
		db:       db,
		key:      key,
		groupKey: channelGroupKey(key),
	}
}
