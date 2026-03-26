package wkdb

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/WuKongIM/wraft/multiraft"
	"github.com/cockroachdb/pebble"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	raftKeyKindHardState   byte = 0x01
	raftKeyKindApplied     byte = 0x02
	raftKeyKindSnapshot    byte = 0x03
	raftKeyKindLogPrefix   byte = 0x10
)

type wkdbRaftStorage struct {
	db    *DB
	group uint64
}

func NewRaftStorage(db *DB, group uint64) multiraft.Storage {
	return &wkdbRaftStorage{
		db:    db,
		group: group,
	}
}

func (s *wkdbRaftStorage) InitialState(ctx context.Context) (multiraft.BootstrapState, error) {
	if err := s.validate(); err != nil {
		return multiraft.BootstrapState{}, err
	}
	if err := s.db.checkContext(ctx); err != nil {
		return multiraft.BootstrapState{}, err
	}

	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	state := multiraft.BootstrapState{}
	if data, err := s.db.getValue(encodeRaftHardStateKey(s.group)); err == nil {
		if err := state.HardState.Unmarshal(data); err != nil {
			return multiraft.BootstrapState{}, err
		}
	} else if !errors.Is(err, ErrNotFound) {
		return multiraft.BootstrapState{}, err
	}

	if data, err := s.db.getValue(encodeRaftAppliedKey(s.group)); err == nil {
		if len(data) != 8 {
			return multiraft.BootstrapState{}, ErrCorruptValue
		}
		state.AppliedIndex = binary.BigEndian.Uint64(data)
	} else if !errors.Is(err, ErrNotFound) {
		return multiraft.BootstrapState{}, err
	}

	snap, err := s.snapshotLocked()
	if err != nil {
		return multiraft.BootstrapState{}, err
	}
	if !raft.IsEmptySnap(snap) {
		state.ConfState = snap.Metadata.ConfState
	}

	return state, nil
}

func (s *wkdbRaftStorage) Entries(ctx context.Context, lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if err := s.db.checkContext(ctx); err != nil {
		return nil, err
	}

	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	iter, err := s.db.db.NewIter(&pebble.IterOptions{
		LowerBound: encodeRaftLogKey(s.group, lo),
		UpperBound: encodeRaftLogKey(s.group, hi),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var entries []raftpb.Entry
	var size uint64
	for iter.First(); iter.Valid(); iter.Next() {
		if err := s.db.checkContext(ctx); err != nil {
			return nil, err
		}
		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, err
		}

		var entry raftpb.Entry
		if err := entry.Unmarshal(value); err != nil {
			return nil, err
		}

		if maxSize > 0 && len(entries) > 0 && size+uint64(entry.Size()) > maxSize {
			break
		}
		size += uint64(entry.Size())
		entries = append(entries, entry)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *wkdbRaftStorage) Term(ctx context.Context, index uint64) (uint64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	if err := s.db.checkContext(ctx); err != nil {
		return 0, err
	}

	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	if data, err := s.db.getValue(encodeRaftLogKey(s.group, index)); err == nil {
		var entry raftpb.Entry
		if err := entry.Unmarshal(data); err != nil {
			return 0, err
		}
		return entry.Term, nil
	} else if !errors.Is(err, ErrNotFound) {
		return 0, err
	}

	snap, err := s.snapshotLocked()
	if err != nil {
		return 0, err
	}
	if snap.Metadata.Index == index {
		return snap.Metadata.Term, nil
	}
	return 0, nil
}

func (s *wkdbRaftStorage) FirstIndex(ctx context.Context) (uint64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	if err := s.db.checkContext(ctx); err != nil {
		return 0, err
	}

	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	iter, err := s.db.db.NewIter(&pebble.IterOptions{
		LowerBound: encodeRaftLogPrefix(s.group),
		UpperBound: nextPrefix(encodeRaftLogPrefix(s.group)),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	if iter.First() {
		entry, err := readRaftEntry(iter)
		if err != nil {
			return 0, err
		}
		return entry.Index, nil
	}
	if err := iter.Error(); err != nil {
		return 0, err
	}

	snap, err := s.snapshotLocked()
	if err != nil {
		return 0, err
	}
	if !raft.IsEmptySnap(snap) {
		return snap.Metadata.Index + 1, nil
	}
	return 1, nil
}

func (s *wkdbRaftStorage) LastIndex(ctx context.Context) (uint64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	if err := s.db.checkContext(ctx); err != nil {
		return 0, err
	}

	s.db.mu.RLock()
	defer s.db.mu.RUnlock()

	iter, err := s.db.db.NewIter(&pebble.IterOptions{
		LowerBound: encodeRaftLogPrefix(s.group),
		UpperBound: nextPrefix(encodeRaftLogPrefix(s.group)),
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	if iter.Last() {
		entry, err := readRaftEntry(iter)
		if err != nil {
			return 0, err
		}
		return entry.Index, nil
	}
	if err := iter.Error(); err != nil {
		return 0, err
	}

	snap, err := s.snapshotLocked()
	if err != nil {
		return 0, err
	}
	return snap.Metadata.Index, nil
}

func (s *wkdbRaftStorage) Snapshot(ctx context.Context) (raftpb.Snapshot, error) {
	if err := s.validate(); err != nil {
		return raftpb.Snapshot{}, err
	}
	if err := s.db.checkContext(ctx); err != nil {
		return raftpb.Snapshot{}, err
	}

	s.db.mu.RLock()
	defer s.db.mu.RUnlock()
	return s.snapshotLocked()
}

func (s *wkdbRaftStorage) Save(ctx context.Context, st multiraft.PersistentState) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := s.db.checkContext(ctx); err != nil {
		return err
	}

	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	batch := s.db.db.NewBatch()
	defer batch.Close()

	if st.HardState != nil {
		data, err := st.HardState.Marshal()
		if err != nil {
			return err
		}
		if err := batch.Set(encodeRaftHardStateKey(s.group), data, nil); err != nil {
			return err
		}
	}

	if st.Snapshot != nil {
		data, err := st.Snapshot.Marshal()
		if err != nil {
			return err
		}
		if err := batch.Set(encodeRaftSnapshotKey(s.group), data, nil); err != nil {
			return err
		}
		if err := batch.DeleteRange(
			encodeRaftLogPrefix(s.group),
			encodeRaftLogKey(s.group, st.Snapshot.Metadata.Index+1),
			nil,
		); err != nil {
			return err
		}
	}

	if len(st.Entries) > 0 {
		first := st.Entries[0].Index
		if err := batch.DeleteRange(
			encodeRaftLogKey(s.group, first),
			nextPrefix(encodeRaftLogPrefix(s.group)),
			nil,
		); err != nil {
			return err
		}

		for _, entry := range st.Entries {
			data, err := entry.Marshal()
			if err != nil {
				return err
			}
			if err := batch.Set(encodeRaftLogKey(s.group, entry.Index), data, nil); err != nil {
				return err
			}
		}
	}

	return batch.Commit(pebble.Sync)
}

func (s *wkdbRaftStorage) MarkApplied(ctx context.Context, index uint64) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := s.db.checkContext(ctx); err != nil {
		return err
	}

	s.db.mu.Lock()
	defer s.db.mu.Unlock()

	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, index)
	return s.db.db.Set(encodeRaftAppliedKey(s.group), value, pebble.Sync)
}

func (s *wkdbRaftStorage) validate() error {
	if s == nil || s.db == nil {
		return ErrInvalidArgument
	}
	return validateSlot(s.group)
}

func (s *wkdbRaftStorage) snapshotLocked() (raftpb.Snapshot, error) {
	data, err := s.db.getValue(encodeRaftSnapshotKey(s.group))
	if errors.Is(err, ErrNotFound) {
		return raftpb.Snapshot{}, nil
	}
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	var snap raftpb.Snapshot
	if err := snap.Unmarshal(data); err != nil {
		return raftpb.Snapshot{}, err
	}
	return snap, nil
}

func encodeRaftPrefix(group uint64) []byte {
	return encodeSlotKeyspacePrefix(keyspaceRaft, group)
}

func encodeRaftHardStateKey(group uint64) []byte {
	key := encodeRaftPrefix(group)
	return append(key, raftKeyKindHardState)
}

func encodeRaftAppliedKey(group uint64) []byte {
	key := encodeRaftPrefix(group)
	return append(key, raftKeyKindApplied)
}

func encodeRaftSnapshotKey(group uint64) []byte {
	key := encodeRaftPrefix(group)
	return append(key, raftKeyKindSnapshot)
}

func encodeRaftLogPrefix(group uint64) []byte {
	key := encodeRaftPrefix(group)
	return append(key, raftKeyKindLogPrefix)
}

func encodeRaftLogKey(group, index uint64) []byte {
	key := encodeRaftLogPrefix(group)
	return binary.BigEndian.AppendUint64(key, index)
}

func readRaftEntry(iter *pebble.Iterator) (raftpb.Entry, error) {
	value, err := iter.ValueAndErr()
	if err != nil {
		return raftpb.Entry{}, err
	}

	var entry raftpb.Entry
	if err := entry.Unmarshal(value); err != nil {
		return raftpb.Entry{}, err
	}
	return entry, nil
}
