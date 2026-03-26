package wkdb

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/WuKongIM/wraft/multiraft"
)

type wkdbStateMachine struct {
	db   *DB
	slot uint64
}

type stateMachineCommand struct {
	Type    string   `json:"type"`
	User    *User    `json:"user,omitempty"`
	Channel *Channel `json:"channel,omitempty"`
}

func NewStateMachine(db *DB, slot uint64) multiraft.StateMachine {
	return &wkdbStateMachine{
		db:   db,
		slot: slot,
	}
}

func encodeUpsertUserCommand(user User) []byte {
	data, _ := json.Marshal(stateMachineCommand{
		Type: "upsert_user",
		User: &user,
	})
	return data
}

func encodeUpsertChannelCommand(channel Channel) []byte {
	data, _ := json.Marshal(stateMachineCommand{
		Type:    "upsert_channel",
		Channel: &channel,
	})
	return data
}

func (m *wkdbStateMachine) Apply(ctx context.Context, cmd multiraft.Command) ([]byte, error) {
	if m == nil || m.db == nil {
		return nil, ErrInvalidArgument
	}

	var decoded stateMachineCommand
	if err := json.Unmarshal(cmd.Data, &decoded); err != nil {
		return nil, ErrCorruptValue
	}

	shard := m.db.ForSlot(m.slot)
	switch decoded.Type {
	case "upsert_user":
		if decoded.User == nil {
			return nil, ErrInvalidArgument
		}
		if _, err := shard.GetUser(ctx, decoded.User.UID); err == nil {
			if err := shard.UpdateUser(ctx, *decoded.User); err != nil {
				return nil, err
			}
		} else if errors.Is(err, ErrNotFound) {
			if err := shard.CreateUser(ctx, *decoded.User); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	case "upsert_channel":
		if decoded.Channel == nil {
			return nil, ErrInvalidArgument
		}
		if _, err := shard.GetChannel(ctx, decoded.Channel.ChannelID, decoded.Channel.ChannelType); err == nil {
			if err := shard.UpdateChannel(ctx, *decoded.Channel); err != nil {
				return nil, err
			}
		} else if errors.Is(err, ErrNotFound) {
			if err := shard.CreateChannel(ctx, *decoded.Channel); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	default:
		return nil, ErrInvalidArgument
	}

	return []byte("ok"), nil
}

func (m *wkdbStateMachine) Restore(ctx context.Context, snap multiraft.Snapshot) error {
	if m == nil || m.db == nil {
		return ErrInvalidArgument
	}
	return m.db.ImportSlotSnapshot(ctx, SlotSnapshot{
		SlotID: m.slot,
		Data:   append([]byte(nil), snap.Data...),
	})
}

func (m *wkdbStateMachine) Snapshot(ctx context.Context) (multiraft.Snapshot, error) {
	if m == nil || m.db == nil {
		return multiraft.Snapshot{}, ErrInvalidArgument
	}

	snap, err := m.db.ExportSlotSnapshot(ctx, m.slot)
	if err != nil {
		return multiraft.Snapshot{}, err
	}
	return multiraft.Snapshot{
		Data: snap.Data,
	}, nil
}
