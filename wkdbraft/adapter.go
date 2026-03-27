package wkdbraft

import (
	"context"

	"github.com/WuKongIM/wraft/multiraft"
	"github.com/WuKongIM/wraft/wkdb"
	"go.etcd.io/raft/v3/raftpb"
)

type storageAdapter struct {
	inner wkdb.RaftStorage
}

func NewStorage(db *wkdb.DB, group uint64) multiraft.Storage {
	return &storageAdapter{inner: wkdb.NewRaftStorage(db, group)}
}

func (a *storageAdapter) InitialState(ctx context.Context) (multiraft.BootstrapState, error) {
	st, err := a.inner.InitialState(ctx)
	if err != nil {
		return multiraft.BootstrapState{}, err
	}
	return multiraft.BootstrapState{
		HardState:    st.HardState,
		ConfState:    st.ConfState,
		AppliedIndex: st.AppliedIndex,
	}, nil
}

func (a *storageAdapter) Entries(ctx context.Context, lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	return a.inner.Entries(ctx, lo, hi, maxSize)
}

func (a *storageAdapter) Term(ctx context.Context, index uint64) (uint64, error) {
	return a.inner.Term(ctx, index)
}

func (a *storageAdapter) FirstIndex(ctx context.Context) (uint64, error) {
	return a.inner.FirstIndex(ctx)
}

func (a *storageAdapter) LastIndex(ctx context.Context) (uint64, error) {
	return a.inner.LastIndex(ctx)
}

func (a *storageAdapter) Snapshot(ctx context.Context) (raftpb.Snapshot, error) {
	return a.inner.Snapshot(ctx)
}

func (a *storageAdapter) Save(ctx context.Context, st multiraft.PersistentState) error {
	return a.inner.Save(ctx, wkdb.RaftPersistentState{
		HardState: st.HardState,
		Entries:   st.Entries,
		Snapshot:  st.Snapshot,
	})
}

func (a *storageAdapter) MarkApplied(ctx context.Context, index uint64) error {
	return a.inner.MarkApplied(ctx, index)
}

type stateMachineAdapter struct {
	inner wkdb.RaftStateMachine
}

func NewStateMachine(db *wkdb.DB, slot uint64) multiraft.StateMachine {
	return &stateMachineAdapter{inner: wkdb.NewStateMachine(db, slot)}
}

func (a *stateMachineAdapter) Apply(ctx context.Context, cmd multiraft.Command) ([]byte, error) {
	return a.inner.Apply(ctx, wkdb.RaftCommand{
		GroupID: uint64(cmd.GroupID),
		Index:   cmd.Index,
		Term:    cmd.Term,
		Data:    cmd.Data,
	})
}

func (a *stateMachineAdapter) Restore(ctx context.Context, snap multiraft.Snapshot) error {
	return a.inner.Restore(ctx, wkdb.RaftSnapshot{
		Index: snap.Index,
		Term:  snap.Term,
		Data:  snap.Data,
	})
}

func (a *stateMachineAdapter) Snapshot(ctx context.Context) (multiraft.Snapshot, error) {
	s, err := a.inner.Snapshot(ctx)
	if err != nil {
		return multiraft.Snapshot{}, err
	}
	return multiraft.Snapshot{
		Index: s.Index,
		Term:  s.Term,
		Data:  s.Data,
	}, nil
}
