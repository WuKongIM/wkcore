package raftstore

import (
	"context"

	"github.com/WuKongIM/wraft/multiraft"
	"github.com/cockroachdb/pebble"
	"go.etcd.io/raft/v3/raftpb"
)

type DB struct {
	db *pebble.DB
}

type pebbleStore struct {
	db    *DB
	group uint64
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

func (db *DB) ForGroup(group uint64) multiraft.Storage {
	return &pebbleStore{
		db:    db,
		group: group,
	}
}

func (s *pebbleStore) InitialState(ctx context.Context) (multiraft.BootstrapState, error) {
	_ = ctx
	return multiraft.BootstrapState{}, nil
}

func (s *pebbleStore) Entries(ctx context.Context, lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	_, _, _ = ctx, lo, hi
	_ = maxSize
	return nil, nil
}

func (s *pebbleStore) Term(ctx context.Context, index uint64) (uint64, error) {
	_, _ = ctx, index
	return 0, nil
}

func (s *pebbleStore) FirstIndex(ctx context.Context) (uint64, error) {
	_ = ctx
	return 1, nil
}

func (s *pebbleStore) LastIndex(ctx context.Context) (uint64, error) {
	_ = ctx
	return 0, nil
}

func (s *pebbleStore) Snapshot(ctx context.Context) (raftpb.Snapshot, error) {
	_ = ctx
	return raftpb.Snapshot{}, nil
}

func (s *pebbleStore) Save(ctx context.Context, st multiraft.PersistentState) error {
	_, _ = ctx, st
	return nil
}

func (s *pebbleStore) MarkApplied(ctx context.Context, index uint64) error {
	_, _ = ctx, index
	return nil
}
