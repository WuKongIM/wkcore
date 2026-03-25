package multiraft

import (
	"context"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

type GroupID uint64
type NodeID uint64

type Options struct {
	NodeID       NodeID
	TickInterval time.Duration
	Workers      int
	Transport    Transport
	Raft         RaftOptions
}

type RaftOptions struct {
	ElectionTick  int
	HeartbeatTick int
	PreVote       bool
	CheckQuorum   bool
	MaxSizePerMsg uint64
	MaxInflight   int
}

type GroupOptions struct {
	ID           GroupID
	Storage      Storage
	StateMachine StateMachine
}

type BootstrapGroupRequest struct {
	Group  GroupOptions
	Voters []NodeID
}

type Envelope struct {
	GroupID GroupID
	Message raftpb.Message
}

type Future interface {
	Wait(ctx context.Context) (Result, error)
}

type Result struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type Status struct {
	GroupID      GroupID
	NodeID       NodeID
	LeaderID     NodeID
	Term         uint64
	CommitIndex  uint64
	AppliedIndex uint64
	Role         Role
}

type Transport interface {
	Send(ctx context.Context, batch []Envelope) error
}

type Storage interface {
	InitialState(ctx context.Context) (BootstrapState, error)
	Entries(ctx context.Context, lo, hi, maxSize uint64) ([]raftpb.Entry, error)
	Term(ctx context.Context, index uint64) (uint64, error)
	FirstIndex(ctx context.Context) (uint64, error)
	LastIndex(ctx context.Context) (uint64, error)
	Snapshot(ctx context.Context) (raftpb.Snapshot, error)

	Save(ctx context.Context, st PersistentState) error
	MarkApplied(ctx context.Context, index uint64) error
}

type BootstrapState struct {
	HardState    raftpb.HardState
	ConfState    raftpb.ConfState
	AppliedIndex uint64
}

type PersistentState struct {
	HardState *raftpb.HardState
	Entries   []raftpb.Entry
	Snapshot  *raftpb.Snapshot
}

type StateMachine interface {
	Apply(ctx context.Context, cmd Command) ([]byte, error)
	Restore(ctx context.Context, snap Snapshot) error
	Snapshot(ctx context.Context) (Snapshot, error)
}

type Command struct {
	GroupID GroupID
	Index   uint64
	Term    uint64
	Data    []byte
}

type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type ConfigChange struct {
	Type    ChangeType
	NodeID  NodeID
	Context []byte
}

type ChangeType uint8

const (
	AddVoter ChangeType = iota + 1
	RemoveVoter
	AddLearner
	PromoteLearner
)

type Role uint8

const (
	RoleFollower Role = iota + 1
	RoleCandidate
	RoleLeader
)
