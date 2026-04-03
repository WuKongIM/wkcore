package channellog

import (
	"context"
	"errors"
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/replication/isr"
)

type appliedMessage struct {
	key   IdempotencyKey
	entry IdempotencyEntry
}

type committingStateStore interface {
	ChannelStateStore
	CommitCommitted(checkpoint isr.Checkpoint, batch []appliedMessage) error
}

type checkpointBridge struct {
	base     isr.CheckpointStore
	log      MessageLog
	key      ChannelKey
	state    ChannelStateStore
	groupKey isr.GroupKey
	prevHW   uint64
}

func newCheckpointBridge(base isr.CheckpointStore, log MessageLog, key ChannelKey, state ChannelStateStore, groupKey isr.GroupKey) (*checkpointBridge, error) {
	checkpoint, err := base.Load()
	if err != nil && !errors.Is(err, isr.ErrEmptyState) {
		return nil, err
	}
	return &checkpointBridge{
		base:     base,
		log:      log,
		key:      key,
		state:    state,
		groupKey: groupKey,
		prevHW:   checkpoint.HW,
	}, nil
}

func (b *checkpointBridge) Load() (isr.Checkpoint, error) {
	return b.base.Load()
}

func (b *checkpointBridge) Store(checkpoint isr.Checkpoint) error {
	batch, err := b.readCommittedBatch(checkpoint.HW)
	if err != nil {
		return err
	}
	if len(batch) > 0 {
		if store, ok := b.state.(committingStateStore); ok {
			if err := store.CommitCommitted(checkpoint, batch); err != nil {
				return err
			}
		} else {
			for _, msg := range batch {
				if err := b.state.PutIdempotency(msg.key, msg.entry); err != nil {
					return err
				}
			}
		}
	}
	if err := b.base.Store(checkpoint); err != nil {
		return err
	}
	b.prevHW = checkpoint.HW
	return nil
}

func (b *checkpointBridge) readCommittedBatch(nextHW uint64) ([]appliedMessage, error) {
	if nextHW <= b.prevHW {
		return nil, nil
	}

	records, err := b.log.Read(b.groupKey, b.prevHW, int(nextHW-b.prevHW), math.MaxInt)
	if err != nil {
		return nil, err
	}

	batch := make([]appliedMessage, 0, len(records))
	for _, record := range records {
		message, err := decodeStoredMessage(record.Payload)
		if err != nil {
			return nil, err
		}
		batch = append(batch, appliedMessage{
			key: IdempotencyKey{
				ChannelID:   b.key.ChannelID,
				ChannelType: b.key.ChannelType,
				SenderUID:   message.SenderUID,
				ClientMsgNo: message.ClientMsgNo,
			},
			entry: IdempotencyEntry{
				MessageID:  message.MessageID,
				MessageSeq: record.Offset + 1,
				Offset:     record.Offset,
			},
		})
	}
	return batch, nil
}

type snapshotBridge struct {
	base  isr.SnapshotApplier
	state ChannelStateStore
}

func newSnapshotBridge(base isr.SnapshotApplier, state ChannelStateStore) *snapshotBridge {
	return &snapshotBridge{
		base:  base,
		state: state,
	}
}

func (b *snapshotBridge) InstallSnapshot(ctx context.Context, snap isr.Snapshot) error {
	if err := b.base.InstallSnapshot(ctx, snap); err != nil {
		return err
	}
	return b.state.Restore(snap.Payload)
}
