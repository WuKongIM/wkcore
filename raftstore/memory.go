package raftstore

import (
	"context"
	"sync"

	"github.com/WuKongIM/wraft/multiraft"
	raft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/confchange"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

type memoryStore struct {
	mu       sync.Mutex
	state    multiraft.BootstrapState
	entries  []raftpb.Entry
	snapshot raftpb.Snapshot
}

func NewMemory() multiraft.Storage {
	return &memoryStore{}
}

func (m *memoryStore) InitialState(ctx context.Context) (multiraft.BootstrapState, error) {
	_ = ctx

	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.state
	confState, err := deriveConfState(m.snapshot, m.entries, state.HardState.Commit)
	if err != nil {
		return multiraft.BootstrapState{}, err
	}
	state.ConfState = confState
	return state, nil
}

func (m *memoryStore) Entries(ctx context.Context, lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	_ = ctx

	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		out  []raftpb.Entry
		size uint64
	)
	for _, entry := range m.entries {
		if entry.Index < lo || entry.Index >= hi {
			continue
		}
		if maxSize > 0 && len(out) > 0 && size+uint64(entry.Size()) > maxSize {
			break
		}
		size += uint64(entry.Size())
		out = append(out, cloneEntry(entry))
	}
	return out, nil
}

func (m *memoryStore) Term(ctx context.Context, index uint64) (uint64, error) {
	_ = ctx

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, entry := range m.entries {
		if entry.Index == index {
			return entry.Term, nil
		}
	}
	if m.snapshot.Metadata.Index == index {
		return m.snapshot.Metadata.Term, nil
	}
	return 0, nil
}

func (m *memoryStore) FirstIndex(ctx context.Context) (uint64, error) {
	_ = ctx

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.entries) > 0 {
		return m.entries[0].Index, nil
	}
	if !raft.IsEmptySnap(m.snapshot) {
		return m.snapshot.Metadata.Index + 1, nil
	}
	return 1, nil
}

func (m *memoryStore) LastIndex(ctx context.Context) (uint64, error) {
	_ = ctx

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.entries) > 0 {
		return m.entries[len(m.entries)-1].Index, nil
	}
	return m.snapshot.Metadata.Index, nil
}

func (m *memoryStore) Snapshot(ctx context.Context) (raftpb.Snapshot, error) {
	_ = ctx

	m.mu.Lock()
	defer m.mu.Unlock()

	return cloneSnapshot(m.snapshot), nil
}

func (m *memoryStore) Save(ctx context.Context, st multiraft.PersistentState) error {
	_ = ctx

	m.mu.Lock()
	defer m.mu.Unlock()

	if st.HardState != nil {
		m.state.HardState = *st.HardState
	}
	if st.Snapshot != nil {
		m.snapshot = cloneSnapshot(*st.Snapshot)
		m.entries = trimEntriesAfterSnapshot(m.entries, st.Snapshot.Metadata.Index)
		if m.state.HardState.Commit < st.Snapshot.Metadata.Index {
			m.state.HardState.Commit = st.Snapshot.Metadata.Index
		}
	}
	if len(st.Entries) > 0 {
		first := st.Entries[0].Index
		m.entries = replaceEntriesFromIndex(m.entries, first, st.Entries)
	}
	return nil
}

func (m *memoryStore) MarkApplied(ctx context.Context, index uint64) error {
	_ = ctx

	m.mu.Lock()
	defer m.mu.Unlock()

	m.state.AppliedIndex = index
	return nil
}

func replaceEntriesFromIndex(existing []raftpb.Entry, first uint64, incoming []raftpb.Entry) []raftpb.Entry {
	result := make([]raftpb.Entry, 0, len(existing)+len(incoming))
	for _, entry := range existing {
		if entry.Index >= first {
			break
		}
		result = append(result, cloneEntry(entry))
	}
	for _, entry := range incoming {
		result = append(result, cloneEntry(entry))
	}
	return result
}

func trimEntriesAfterSnapshot(existing []raftpb.Entry, snapshotIndex uint64) []raftpb.Entry {
	result := make([]raftpb.Entry, 0, len(existing))
	for _, entry := range existing {
		if entry.Index <= snapshotIndex {
			continue
		}
		result = append(result, cloneEntry(entry))
	}
	return result
}

func cloneEntry(entry raftpb.Entry) raftpb.Entry {
	cloned := entry
	if len(entry.Data) > 0 {
		cloned.Data = append([]byte(nil), entry.Data...)
	}
	return cloned
}

func cloneSnapshot(snapshot raftpb.Snapshot) raftpb.Snapshot {
	cloned := snapshot
	if len(snapshot.Data) > 0 {
		cloned.Data = append([]byte(nil), snapshot.Data...)
	}
	cloned.Metadata.ConfState = cloneConfState(snapshot.Metadata.ConfState)
	return cloned
}

func cloneConfState(state raftpb.ConfState) raftpb.ConfState {
	cloned := state
	if len(state.Voters) > 0 {
		cloned.Voters = append([]uint64(nil), state.Voters...)
	}
	if len(state.Learners) > 0 {
		cloned.Learners = append([]uint64(nil), state.Learners...)
	}
	if len(state.VotersOutgoing) > 0 {
		cloned.VotersOutgoing = append([]uint64(nil), state.VotersOutgoing...)
	}
	if len(state.LearnersNext) > 0 {
		cloned.LearnersNext = append([]uint64(nil), state.LearnersNext...)
	}
	return cloned
}

func deriveConfState(snapshot raftpb.Snapshot, entries []raftpb.Entry, committed uint64) (raftpb.ConfState, error) {
	var (
		base       raftpb.ConfState
		lastIndex  uint64
		progress   = tracker.MakeProgressTracker(1, 0)
		cfg        tracker.Config
		progresses tracker.ProgressMap
		err        error
	)

	if !raft.IsEmptySnap(snapshot) {
		base = cloneConfState(snapshot.Metadata.ConfState)
		lastIndex = snapshot.Metadata.Index
	}

	if !isZeroConfState(base) {
		cfg, progresses, err = confchange.Restore(confchange.Changer{
			Tracker:   progress,
			LastIndex: lastIndex,
		}, base)
		if err != nil {
			return raftpb.ConfState{}, err
		}
		progress.Config = cfg
		progress.Progress = progresses
	}

	if committed < lastIndex {
		committed = lastIndex
	}
	for _, entry := range entries {
		if entry.Index <= lastIndex || entry.Index > committed || entry.Type != raftpb.EntryConfChange {
			continue
		}

		var cc raftpb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			return raftpb.ConfState{}, err
		}

		nextCfg, nextProgress, err := applyConfChange(progress, entry.Index, cc.AsV2())
		if err != nil {
			return raftpb.ConfState{}, err
		}
		progress.Config = nextCfg
		progress.Progress = nextProgress
	}

	return cloneConfState(progress.ConfState()), nil
}

func applyConfChange(
	progress tracker.ProgressTracker,
	lastIndex uint64,
	change raftpb.ConfChangeV2,
) (tracker.Config, tracker.ProgressMap, error) {
	changer := confchange.Changer{
		Tracker:   progress,
		LastIndex: lastIndex,
	}

	if change.LeaveJoint() {
		return changer.LeaveJoint()
	}
	if autoLeave, ok := change.EnterJoint(); ok {
		return changer.EnterJoint(autoLeave, change.Changes...)
	}
	return changer.Simple(change.Changes...)
}

func isZeroConfState(state raftpb.ConfState) bool {
	return len(state.Voters) == 0 &&
		len(state.Learners) == 0 &&
		len(state.VotersOutgoing) == 0 &&
		len(state.LearnersNext) == 0 &&
		!state.AutoLeave
}
