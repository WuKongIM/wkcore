package raftstore

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/WuKongIM/wraft/multiraft"
	"github.com/cockroachdb/pebble"
	raft "go.etcd.io/raft/v3"
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
	entries, err := s.loadEntries(0, 0)
	if err != nil {
		return multiraft.BootstrapState{}, err
	}
	confState, err := deriveConfState(snap, nil, hs.Commit)
	if err != nil {
		return multiraft.BootstrapState{}, err
	}
	if len(entries) > 0 {
		confState, err = deriveConfState(snap, entries, hs.Commit)
		if err != nil {
			return multiraft.BootstrapState{}, err
		}
	}
	return multiraft.BootstrapState{
		HardState:    hs,
		ConfState:    confState,
		AppliedIndex: appliedIndex,
	}, nil
}

func (s *pebbleStore) Entries(ctx context.Context, lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	_ = ctx

	entries, err := s.loadEntries(lo, hi)
	if err != nil {
		return nil, err
	}

	var (
		out  []raftpb.Entry
		size uint64
	)
	for _, entry := range entries {
		if maxSize > 0 && len(out) > 0 && size+uint64(entry.Size()) > maxSize {
			break
		}
		size += uint64(entry.Size())
		out = append(out, cloneEntry(entry))
	}
	return out, nil
}

func (s *pebbleStore) Term(ctx context.Context, index uint64) (uint64, error) {
	_ = ctx

	var entry raftpb.Entry
	if err := s.loadProto(encodeEntryKey(s.group, index), &entry); err != nil {
		return 0, err
	}
	if entry.Index == index {
		return entry.Term, nil
	}

	snap, err := s.loadSnapshot()
	if err != nil {
		return 0, err
	}
	if snap.Metadata.Index == index {
		return snap.Metadata.Term, nil
	}
	return 0, nil
}

func (s *pebbleStore) FirstIndex(ctx context.Context) (uint64, error) {
	_ = ctx

	iter, err := s.db.db.NewIter(&pebble.IterOptions{
		LowerBound: encodeEntryPrefix(s.group),
		UpperBound: encodeEntryPrefixEnd(s.group),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	if iter.First() {
		entry, err := decodeEntryValue(iter)
		if err != nil {
			return 0, err
		}
		return entry.Index, nil
	}

	snap, err := s.loadSnapshot()
	if err != nil {
		return 0, err
	}
	if !raft.IsEmptySnap(snap) {
		return snap.Metadata.Index + 1, nil
	}
	return 1, nil
}

func (s *pebbleStore) LastIndex(ctx context.Context) (uint64, error) {
	_ = ctx

	iter, err := s.db.db.NewIter(&pebble.IterOptions{
		LowerBound: encodeEntryPrefix(s.group),
		UpperBound: encodeEntryPrefixEnd(s.group),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	if iter.Last() {
		entry, err := decodeEntryValue(iter)
		if err != nil {
			return 0, err
		}
		return entry.Index, nil
	}

	snap, err := s.loadSnapshot()
	if err != nil {
		return 0, err
	}
	return snap.Metadata.Index, nil
}

func (s *pebbleStore) Snapshot(ctx context.Context) (raftpb.Snapshot, error) {
	_ = ctx
	return s.loadSnapshot()
}

func (s *pebbleStore) Save(ctx context.Context, st multiraft.PersistentState) error {
	_ = ctx

	batch := s.db.db.NewBatch()
	defer batch.Close()

	hs := raftpb.HardState{}
	persistHardState := false
	if st.HardState != nil {
		hs = *st.HardState
		persistHardState = true
	} else if st.Snapshot != nil {
		var err error
		hs, err = s.loadHardState()
		if err != nil {
			return err
		}
		persistHardState = true
	}

	if st.Snapshot != nil {
		data, err := st.Snapshot.Marshal()
		if err != nil {
			return err
		}
		if err := batch.Set(encodeSnapshotKey(s.group), data, nil); err != nil {
			return err
		}
		if st.Snapshot.Metadata.Index < ^uint64(0) {
			if err := batch.DeleteRange(
				encodeEntryPrefix(s.group),
				encodeEntryKey(s.group, st.Snapshot.Metadata.Index+1),
				nil,
			); err != nil {
				return err
			}
		} else {
			if err := batch.DeleteRange(
				encodeEntryPrefix(s.group),
				encodeEntryPrefixEnd(s.group),
				nil,
			); err != nil {
				return err
			}
		}
		if hs.Commit < st.Snapshot.Metadata.Index {
			hs.Commit = st.Snapshot.Metadata.Index
		}
	}

	if len(st.Entries) > 0 {
		first := st.Entries[0].Index
		if err := batch.DeleteRange(
			encodeEntryKey(s.group, first),
			encodeEntryPrefixEnd(s.group),
			nil,
		); err != nil {
			return err
		}
		for _, entry := range st.Entries {
			data, err := entry.Marshal()
			if err != nil {
				return err
			}
			if err := batch.Set(encodeEntryKey(s.group, entry.Index), data, nil); err != nil {
				return err
			}
		}
	}

	if persistHardState {
		data, err := hs.Marshal()
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

func (s *pebbleStore) loadEntries(lo, hi uint64) ([]raftpb.Entry, error) {
	lower := encodeEntryPrefix(s.group)
	upper := encodeEntryPrefixEnd(s.group)
	if lo > 0 {
		lower = encodeEntryKey(s.group, lo)
	}
	if hi > 0 {
		upper = encodeEntryKey(s.group, hi)
	}

	iter, err := s.db.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var entries []raftpb.Entry
	for iter.First(); iter.Valid(); iter.Next() {
		entry, err := decodeEntryValue(iter)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return entries, nil
}

type iterValueReader interface {
	ValueAndErr() ([]byte, error)
}

func decodeEntryValue(iter iterValueReader) (raftpb.Entry, error) {
	value, err := iter.ValueAndErr()
	if err != nil {
		return raftpb.Entry{}, err
	}
	var entry raftpb.Entry
	if err := entry.Unmarshal(value); err != nil {
		return raftpb.Entry{}, err
	}
	return cloneEntry(entry), nil
}
