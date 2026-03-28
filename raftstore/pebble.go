package raftstore

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync"

	"github.com/WuKongIM/wraft/multiraft"
	"github.com/cockroachdb/pebble/v2"
	raft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	// groupMetaHeaderSize is the fixed-size header of the serialised groupMeta:
	// 5 uint64 fields (8 bytes each) + 1 uint32 confState length = 44 bytes.
	groupMetaHeaderSize = 5*8 + 4

	// appliedIndexSize is the byte length of the persisted applied index value.
	appliedIndexSize = 8

	// defaultWriteChSize is the channel buffer size for batched write requests.
	defaultWriteChSize = 1024
)

type DB struct {
	db *pebble.DB

	mu      sync.Mutex
	closing bool

	writeCh  chan *writeRequest
	workerWG sync.WaitGroup

	stateCache map[uint64]groupWriteState
}

type groupMeta struct {
	FirstIndex    uint64
	LastIndex     uint64
	AppliedIndex  uint64
	SnapshotIndex uint64
	SnapshotTerm  uint64
	ConfState     raftpb.ConfState
}

type pebbleStore struct {
	db    *DB
	group uint64
}

type writeRequest struct {
	group        uint64
	state        *multiraft.PersistentState
	appliedIndex *uint64
	done         chan error
}

type groupWriteState struct {
	hardState raftpb.HardState
	snapshot  raftpb.Snapshot
	entries   []raftpb.Entry
	meta      groupMeta
}

func Open(path string) (*DB, error) {
	pdb, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}

	db := &DB{
		db:         pdb,
		writeCh:    make(chan *writeRequest, defaultWriteChSize),
		stateCache: make(map[uint64]groupWriteState),
	}
	db.workerWG.Add(1)
	go db.runWriteWorker()
	return db, nil
}

func (db *DB) Close() error {
	if db == nil || db.db == nil {
		return nil
	}

	db.mu.Lock()
	if db.closing {
		db.mu.Unlock()
		return nil
	}
	db.closing = true
	close(db.writeCh)
	db.mu.Unlock()

	db.workerWG.Wait()

	err := db.db.Close()
	db.db = nil
	return err
}

func (db *DB) ForGroup(group uint64) multiraft.Storage {
	return &pebbleStore{
		db:    db,
		group: group,
	}
}

func (s *pebbleStore) InitialState(ctx context.Context) (multiraft.BootstrapState, error) {
	_ = ctx

	meta, ok, err := s.ensureGroupMeta()
	if err != nil {
		return multiraft.BootstrapState{}, err
	}
	if ok {
		hs, err := s.loadHardState()
		if err != nil {
			return multiraft.BootstrapState{}, err
		}
		return multiraft.BootstrapState{
			HardState:    hs,
			ConfState:    cloneConfState(meta.ConfState),
			AppliedIndex: meta.AppliedIndex,
		}, nil
	}

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
	confState, err := deriveConfState(snap, entries, hs.Commit)
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

	meta, ok, err := s.ensureGroupMeta()
	if err != nil {
		return 0, err
	}
	if ok {
		return meta.FirstIndex, nil
	}

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

	meta, ok, err := s.ensureGroupMeta()
	if err != nil {
		return 0, err
	}
	if ok {
		return meta.LastIndex, nil
	}

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

	req := &writeRequest{
		group: s.group,
		state: &st,
		done:  make(chan error, 1),
	}
	return s.db.submitWrite(req)
}

func (s *pebbleStore) MarkApplied(ctx context.Context, index uint64) error {
	_ = ctx

	req := &writeRequest{
		group:        s.group,
		appliedIndex: &index,
		done:         make(chan error, 1),
	}
	return s.db.submitWrite(req)
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
	if len(value) != appliedIndexSize {
		return 0, errors.New("raftstore: invalid applied index encoding")
	}
	return binary.BigEndian.Uint64(value), nil
}

func (m groupMeta) Marshal() ([]byte, error) {
	confState, err := m.ConfState.Marshal()
	if err != nil {
		return nil, err
	}

	data := make([]byte, 0, groupMetaHeaderSize+len(confState))
	data = binary.BigEndian.AppendUint64(data, m.FirstIndex)
	data = binary.BigEndian.AppendUint64(data, m.LastIndex)
	data = binary.BigEndian.AppendUint64(data, m.AppliedIndex)
	data = binary.BigEndian.AppendUint64(data, m.SnapshotIndex)
	data = binary.BigEndian.AppendUint64(data, m.SnapshotTerm)
	data = binary.BigEndian.AppendUint32(data, uint32(len(confState)))
	data = append(data, confState...)
	return data, nil
}

func (m *groupMeta) Unmarshal(data []byte) error {
	if len(data) < groupMetaHeaderSize {
		return errors.New("raftstore: invalid group metadata encoding")
	}

	m.FirstIndex = binary.BigEndian.Uint64(data[0:8])
	m.LastIndex = binary.BigEndian.Uint64(data[8:16])
	m.AppliedIndex = binary.BigEndian.Uint64(data[16:24])
	m.SnapshotIndex = binary.BigEndian.Uint64(data[24:32])
	m.SnapshotTerm = binary.BigEndian.Uint64(data[32:40])

	confStateSize := binary.BigEndian.Uint32(data[40:44])
	if len(data[44:]) != int(confStateSize) {
		return errors.New("raftstore: invalid group metadata conf state size")
	}

	var confState raftpb.ConfState
	if confStateSize > 0 {
		if err := confState.Unmarshal(data[44:]); err != nil {
			return err
		}
	}
	m.ConfState = cloneConfState(confState)
	return nil
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

func (s *pebbleStore) loadGroupMeta() (groupMeta, bool, error) {
	value, err := s.getValue(encodeGroupStateKey(s.group))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return groupMeta{}, false, nil
		}
		return groupMeta{}, false, err
	}

	var meta groupMeta
	if err := meta.Unmarshal(value); err != nil {
		return groupMeta{}, false, err
	}
	return meta, true, nil
}

// currentGroupMeta returns the group metadata. If persisted metadata exists on
// disk it is returned with fromDisk=true. Otherwise the metadata is derived
// from the raw snapshot, entries and applied index (fromDisk=false).
func (s *pebbleStore) currentGroupMeta() (meta groupMeta, fromDisk bool, err error) {
	meta, ok, err := s.loadGroupMeta()
	if err != nil {
		return groupMeta{}, false, err
	}
	if ok {
		return meta, true, nil
	}

	snap, err := s.loadSnapshot()
	if err != nil {
		return groupMeta{}, false, err
	}
	hs, err := s.loadHardState()
	if err != nil {
		return groupMeta{}, false, err
	}
	appliedIndex, err := s.loadAppliedIndex()
	if err != nil {
		return groupMeta{}, false, err
	}
	entries, err := s.loadEntries(0, 0)
	if err != nil {
		return groupMeta{}, false, err
	}

	meta = groupMeta{AppliedIndex: appliedIndex}
	if err := updateGroupMeta(&meta, snap, entries, hs.Commit); err != nil {
		return groupMeta{}, false, err
	}
	return meta, false, nil
}

// ensureGroupMeta returns the group metadata, persisting it if it was derived
// rather than loaded from disk. This guarantees that subsequent reads hit the
// fast path.
func (s *pebbleStore) ensureGroupMeta() (groupMeta, bool, error) {
	meta, fromDisk, err := s.currentGroupMeta()
	if err != nil {
		return groupMeta{}, false, err
	}
	if fromDisk {
		return meta, true, nil
	}
	if err := s.persistGroupMeta(meta); err != nil {
		return groupMeta{}, false, err
	}
	return meta, true, nil
}

func (s *pebbleStore) setGroupMeta(batch *pebble.Batch, meta groupMeta) error {
	data, err := meta.Marshal()
	if err != nil {
		return err
	}
	return batch.Set(encodeGroupStateKey(s.group), data, nil)
}

func (s *pebbleStore) persistGroupMeta(meta groupMeta) error {
	data, err := meta.Marshal()
	if err != nil {
		return err
	}
	return s.db.db.Set(encodeGroupStateKey(s.group), data, pebble.Sync)
}

func (db *DB) submitWrite(req *writeRequest) error {
	db.mu.Lock()
	if db.closing {
		db.mu.Unlock()
		return errors.New("raftstore: db closing")
	}
	db.writeCh <- req
	db.mu.Unlock()
	return <-req.done
}

func (db *DB) runWriteWorker() {
	defer db.workerWG.Done()

	for {
		req, ok := <-db.writeCh
		if !ok {
			return
		}

		reqs := []*writeRequest{req}
		closed := false
		for {
			select {
			case next, ok := <-db.writeCh:
				if !ok {
					closed = true
					goto flush
				}
				reqs = append(reqs, next)
			default:
				goto flush
			}
		}

	flush:
		err := db.flushWriteRequests(reqs)
		for _, req := range reqs {
			req.done <- err
			close(req.done)
		}
		if closed {
			return
		}
	}
}

func (db *DB) flushWriteRequests(reqs []*writeRequest) error {
	batch := db.db.NewBatch()
	defer batch.Close()

	stateCache := make(map[uint64]*groupWriteState, len(reqs))
	for _, req := range reqs {
		state, err := db.loadGroupWriteState(stateCache, req.group)
		if err != nil {
			return err
		}
		if req.state != nil {
			if err := db.applySaveToBatch(batch, req.group, state, *req.state); err != nil {
				return err
			}
		}
		if req.appliedIndex != nil {
			if err := db.applyMarkAppliedToBatch(batch, req.group, state, *req.appliedIndex); err != nil {
				return err
			}
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	for group, state := range stateCache {
		db.stateCache[group] = cloneGroupWriteState(*state)
	}
	return nil
}

func (db *DB) loadGroupWriteState(cache map[uint64]*groupWriteState, group uint64) (*groupWriteState, error) {
	if state, ok := cache[group]; ok {
		return state, nil
	}

	if cached, ok := db.stateCache[group]; ok {
		state := cloneGroupWriteState(cached)
		cache[group] = &state
		return &state, nil
	}

	store := &pebbleStore{db: db, group: group}
	meta, _, err := store.currentGroupMeta()
	if err != nil {
		return nil, err
	}
	snapshot, err := store.loadSnapshot()
	if err != nil {
		return nil, err
	}
	hardState, err := store.loadHardState()
	if err != nil {
		return nil, err
	}
	entries, err := store.loadEntries(0, 0)
	if err != nil {
		return nil, err
	}

	state := &groupWriteState{
		hardState: hardState,
		snapshot:  snapshot,
		entries:   entries,
		meta:      meta,
	}
	cache[group] = state
	return state, nil
}

func (db *DB) applySaveToBatch(batch *pebble.Batch, group uint64, state *groupWriteState, st multiraft.PersistentState) error {
	hs := state.hardState
	persistHardState := false
	if st.HardState != nil {
		hs = *st.HardState
		persistHardState = true
	} else if st.Snapshot != nil {
		persistHardState = true
	}

	if st.Snapshot != nil {
		data, err := st.Snapshot.Marshal()
		if err != nil {
			return err
		}
		if err := batch.Set(encodeSnapshotKey(group), data, nil); err != nil {
			return err
		}
		if st.Snapshot.Metadata.Index < math.MaxUint64 {
			if err := batch.DeleteRange(
				encodeEntryPrefix(group),
				encodeEntryKey(group, st.Snapshot.Metadata.Index+1),
				nil,
			); err != nil {
				return err
			}
		} else {
			if err := batch.DeleteRange(
				encodeEntryPrefix(group),
				encodeEntryPrefixEnd(group),
				nil,
			); err != nil {
				return err
			}
		}
		if hs.Commit < st.Snapshot.Metadata.Index {
			hs.Commit = st.Snapshot.Metadata.Index
		}
		state.snapshot = cloneSnapshot(*st.Snapshot)
		state.entries = trimEntriesAfterSnapshot(state.entries, st.Snapshot.Metadata.Index)
	}

	if len(st.Entries) > 0 {
		first := st.Entries[0].Index
		if err := batch.DeleteRange(
			encodeEntryKey(group, first),
			encodeEntryPrefixEnd(group),
			nil,
		); err != nil {
			return err
		}
		for _, entry := range st.Entries {
			data, err := entry.Marshal()
			if err != nil {
				return err
			}
			if err := batch.Set(encodeEntryKey(group, entry.Index), data, nil); err != nil {
				return err
			}
		}
		state.entries = replaceEntriesFromIndex(state.entries, first, st.Entries)
	}

	if persistHardState {
		data, err := hs.Marshal()
		if err != nil {
			return err
		}
		if err := batch.Set(encodeHardStateKey(group), data, nil); err != nil {
			return err
		}
	}

	state.hardState = hs
	if err := updateGroupMeta(&state.meta, state.snapshot, state.entries, state.hardState.Commit); err != nil {
		return err
	}
	return (&pebbleStore{db: db, group: group}).setGroupMeta(batch, state.meta)
}

func (db *DB) applyMarkAppliedToBatch(batch *pebble.Batch, group uint64, state *groupWriteState, index uint64) error {
	value := make([]byte, appliedIndexSize)
	binary.BigEndian.PutUint64(value, index)
	if err := batch.Set(encodeAppliedIndexKey(group), value, nil); err != nil {
		return err
	}

	state.meta.AppliedIndex = index
	return (&pebbleStore{db: db, group: group}).setGroupMeta(batch, state.meta)
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

func updateGroupMeta(meta *groupMeta, snapshot raftpb.Snapshot, entries []raftpb.Entry, committed uint64) error {
	if meta == nil {
		return nil
	}

	meta.SnapshotIndex = snapshot.Metadata.Index
	meta.SnapshotTerm = snapshot.Metadata.Term

	confState, err := deriveConfState(snapshot, entries, committed)
	if err != nil {
		return err
	}
	meta.ConfState = cloneConfState(confState)

	switch {
	case len(entries) > 0:
		meta.FirstIndex = entries[0].Index
		meta.LastIndex = entries[len(entries)-1].Index
	case !raft.IsEmptySnap(snapshot):
		meta.FirstIndex = snapshot.Metadata.Index + 1
		meta.LastIndex = snapshot.Metadata.Index
	default:
		meta.FirstIndex = 1
		meta.LastIndex = 0
	}

	return nil
}

func cloneGroupWriteState(state groupWriteState) groupWriteState {
	cloned := groupWriteState{
		hardState: state.hardState,
		snapshot:  cloneSnapshot(state.snapshot),
		meta: groupMeta{
			FirstIndex:    state.meta.FirstIndex,
			LastIndex:     state.meta.LastIndex,
			AppliedIndex:  state.meta.AppliedIndex,
			SnapshotIndex: state.meta.SnapshotIndex,
			SnapshotTerm:  state.meta.SnapshotTerm,
			ConfState:     cloneConfState(state.meta.ConfState),
		},
	}
	if len(state.entries) > 0 {
		cloned.entries = make([]raftpb.Entry, len(state.entries))
		for i, entry := range state.entries {
			cloned.entries[i] = cloneEntry(entry)
		}
	}
	return cloned
}
