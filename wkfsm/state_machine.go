package wkfsm

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/WuKongIM/wraft/multiraft"
	"github.com/WuKongIM/wraft/wkdb"
)

type stateMachine struct {
	db   *wkdb.DB
	slot uint64
}

func New(db *wkdb.DB, slot uint64) multiraft.StateMachine {
	return &stateMachine{
		db:   db,
		slot: slot,
	}
}

func (m *stateMachine) Apply(ctx context.Context, cmd multiraft.Command) ([]byte, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	if cmd.GroupID != multiraft.GroupID(m.slot) {
		return nil, wkdb.ErrInvalidArgument
	}

	var decoded commandEnvelope
	if err := json.Unmarshal(cmd.Data, &decoded); err != nil {
		return nil, wkdb.ErrCorruptValue
	}

	shard := m.db.ForSlot(m.slot)
	switch decoded.Type {
	case "upsert_user":
		if decoded.User == nil {
			return nil, wkdb.ErrInvalidArgument
		}
		if _, err := shard.GetUser(ctx, decoded.User.UID); err == nil {
			if err := shard.UpdateUser(ctx, *decoded.User); err != nil {
				return nil, err
			}
		} else if errors.Is(err, wkdb.ErrNotFound) {
			if err := shard.CreateUser(ctx, *decoded.User); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	case "upsert_channel":
		if decoded.Channel == nil {
			return nil, wkdb.ErrInvalidArgument
		}
		if _, err := shard.GetChannel(ctx, decoded.Channel.ChannelID, decoded.Channel.ChannelType); err == nil {
			if err := shard.UpdateChannel(ctx, *decoded.Channel); err != nil {
				return nil, err
			}
		} else if errors.Is(err, wkdb.ErrNotFound) {
			if err := shard.CreateChannel(ctx, *decoded.Channel); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	default:
		return nil, wkdb.ErrInvalidArgument
	}

	return []byte("ok"), nil
}

func (m *stateMachine) Restore(ctx context.Context, snap multiraft.Snapshot) error {
	if err := m.validate(); err != nil {
		return err
	}
	return m.db.ImportSlotSnapshot(ctx, wkdb.SlotSnapshot{
		SlotID: m.slot,
		Data:   append([]byte(nil), snap.Data...),
	})
}

func (m *stateMachine) Snapshot(ctx context.Context) (multiraft.Snapshot, error) {
	if err := m.validate(); err != nil {
		return multiraft.Snapshot{}, err
	}

	snap, err := m.db.ExportSlotSnapshot(ctx, m.slot)
	if err != nil {
		return multiraft.Snapshot{}, err
	}
	return multiraft.Snapshot{
		Data: append([]byte(nil), snap.Data...),
	}, nil
}

func (m *stateMachine) validate() error {
	if m == nil || m.db == nil || m.slot == 0 {
		return wkdb.ErrInvalidArgument
	}
	return nil
}
