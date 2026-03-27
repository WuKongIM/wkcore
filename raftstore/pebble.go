package raftstore

import (
	"context"
	"encoding/binary"
	"errors"

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

	snap, err := s.loadSnapshot()
	if err != nil {
		return multiraft.BootstrapState{}, err
	}
	hs, err := s.loadHardState()
	if err != nil {
		return multiraft.BootstrapState{}, err
	}
	appliedIndex, err := s.loadAppliedIndex()
	if err != nil {
		return multiraft.BootstrapState{}, err
	}
	confState, err := deriveConfState(snap, nil, hs.Commit)
	if err != nil {
		return multiraft.BootstrapState{}, err
	}
	return multiraft.BootstrapState{
		HardState:    hs,
		ConfState:    confState,
		AppliedIndex: appliedIndex,
	}, nil
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
	return s.loadSnapshot()
}

func (s *pebbleStore) Save(ctx context.Context, st multiraft.PersistentState) error {
	_ = ctx

	batch := s.db.db.NewBatch()
	defer batch.Close()

	if st.Snapshot != nil {
		data, err := st.Snapshot.Marshal()
		if err != nil {
			return err
		}
		if err := batch.Set(encodeSnapshotKey(s.group), data, nil); err != nil {
			return err
		}
	}
	if st.HardState != nil {
		data, err := st.HardState.Marshal()
		if err != nil {
			return err
		}
		if err := batch.Set(encodeHardStateKey(s.group), data, nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

func (s *pebbleStore) MarkApplied(ctx context.Context, index uint64) error {
	_ = ctx

	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, index)

	batch := s.db.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(encodeAppliedIndexKey(s.group), value, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (s *pebbleStore) loadHardState() (raftpb.HardState, error) {
	var hs raftpb.HardState
	if err := s.loadProto(encodeHardStateKey(s.group), &hs); err != nil {
		return raftpb.HardState{}, err
	}
	return hs, nil
}

func (s *pebbleStore) loadSnapshot() (raftpb.Snapshot, error) {
	var snap raftpb.Snapshot
	if err := s.loadProto(encodeSnapshotKey(s.group), &snap); err != nil {
		return raftpb.Snapshot{}, err
	}
	return cloneSnapshot(snap), nil
}

func (s *pebbleStore) loadAppliedIndex() (uint64, error) {
	value, err := s.getValue(encodeAppliedIndexKey(s.group))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	if len(value) != 8 {
		return 0, errors.New("raftstore: invalid applied index encoding")
	}
	return binary.BigEndian.Uint64(value), nil
}

type unmarshaler interface {
	Unmarshal(data []byte) error
}

func (s *pebbleStore) loadProto(key []byte, msg unmarshaler) error {
	value, err := s.getValue(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil
		}
		return err
	}
	return msg.Unmarshal(value)
}

func (s *pebbleStore) getValue(key []byte) ([]byte, error) {
	value, closer, err := s.db.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return append([]byte(nil), value...), nil
}
