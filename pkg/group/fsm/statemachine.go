package fsm

import (
	"context"
	"fmt"

	metadb "github.com/WuKongIM/WuKongIM/pkg/group/meta"
	"github.com/WuKongIM/WuKongIM/pkg/group/multiraft"
)

// Compile-time interface assertion.
var _ multiraft.BatchStateMachine = (*stateMachine)(nil)

type stateMachine struct {
	db   *metadb.DB
	slot uint64
}

// NewStateMachine creates a state machine for the given slot.
// It returns an error if db is nil or slot is zero.
func NewStateMachine(db *metadb.DB, slot uint64) (multiraft.StateMachine, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: db must not be nil", metadb.ErrInvalidArgument)
	}
	if slot == 0 {
		return nil, fmt.Errorf("%w: slot must not be zero", metadb.ErrInvalidArgument)
	}
	return &stateMachine{
		db:   db,
		slot: slot,
	}, nil
}

// Apply delegates to ApplyBatch with a single-element slice.
func (m *stateMachine) Apply(ctx context.Context, cmd multiraft.Command) ([]byte, error) {
	results, err := m.ApplyBatch(ctx, []multiraft.Command{cmd})
	if err != nil {
		return nil, err
	}
	return results[0], nil
}

func (m *stateMachine) ApplyBatch(ctx context.Context, cmds []multiraft.Command) ([][]byte, error) {
	wb := m.db.NewWriteBatch()
	defer wb.Close()

	results := make([][]byte, len(cmds))
	for i, cmd := range cmds {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if cmd.GroupID != multiraft.GroupID(m.slot) {
			return nil, metadb.ErrInvalidArgument
		}

		decoded, err := decodeCommand(cmd.Data)
		if err != nil {
			return nil, err
		}
		if err := decoded.apply(wb, m.slot); err != nil {
			return nil, err
		}
		results[i] = []byte(ApplyResultOK)
	}

	if err := wb.Commit(); err != nil {
		return nil, err
	}
	return results, nil
}

func (m *stateMachine) Restore(ctx context.Context, snap multiraft.Snapshot) error {
	return m.db.ImportSlotSnapshot(ctx, metadb.SlotSnapshot{
		SlotID: m.slot,
		Data:   append([]byte(nil), snap.Data...),
	})
}

func (m *stateMachine) Snapshot(ctx context.Context) (multiraft.Snapshot, error) {
	snap, err := m.db.ExportSlotSnapshot(ctx, m.slot)
	if err != nil {
		return multiraft.Snapshot{}, err
	}
	return multiraft.Snapshot{
		Data: append([]byte(nil), snap.Data...),
	}, nil
}

// NewStateMachineFactory returns a factory function suitable for raftcluster.Config.NewStateMachine.
func NewStateMachineFactory(db *metadb.DB) func(groupID multiraft.GroupID) (multiraft.StateMachine, error) {
	return func(groupID multiraft.GroupID) (multiraft.StateMachine, error) {
		return NewStateMachine(db, uint64(groupID))
	}
}
