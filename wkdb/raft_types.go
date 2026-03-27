package wkdb

import (
	"context"

	"go.etcd.io/raft/v3/raftpb"
)

type RaftCommand struct {
	GroupID uint64
	Index   uint64
	Term    uint64
	Data    []byte
}

type RaftSnapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type RaftBootstrapState struct {
	HardState    raftpb.HardState
	ConfState    raftpb.ConfState
	AppliedIndex uint64
}

type RaftPersistentState struct {
	HardState *raftpb.HardState
	Entries   []raftpb.Entry
	Snapshot  *raftpb.Snapshot
}

type RaftStateMachine interface {
	Apply(ctx context.Context, cmd RaftCommand) ([]byte, error)
	Restore(ctx context.Context, snap RaftSnapshot) error
	Snapshot(ctx context.Context) (RaftSnapshot, error)
}

type RaftStorage interface {
	InitialState(ctx context.Context) (RaftBootstrapState, error)
	Entries(ctx context.Context, lo, hi, maxSize uint64) ([]raftpb.Entry, error)
	Term(ctx context.Context, index uint64) (uint64, error)
	FirstIndex(ctx context.Context) (uint64, error)
	LastIndex(ctx context.Context) (uint64, error)
	Snapshot(ctx context.Context) (raftpb.Snapshot, error)
	Save(ctx context.Context, st RaftPersistentState) error
	MarkApplied(ctx context.Context, index uint64) error
}
