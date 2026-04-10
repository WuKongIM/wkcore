package log

import (
	"context"
	"errors"
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
	"github.com/cockroachdb/pebble/v2"
)

type appliedMessage struct {
	key   IdempotencyKey
	entry IdempotencyEntry
}

type committingStateStore interface {
	ChannelStateStore
	CommitCommitted(checkpoint isr.Checkpoint, batch []appliedMessage) error
}

type atomicCheckpointStateStore interface {
	ChannelStateStore
	CommitCommittedWithCheckpoint(checkpoint isr.Checkpoint, batch []appliedMessage) error
}

type batchCheckpointStateStore interface {
	ChannelStateStore
	BuildCommitCommittedWithCheckpoint(writeBatch *pebble.Batch, checkpoint isr.Checkpoint, batch []appliedMessage) error
}

type checkpointBridge struct {
	base     isr.CheckpointStore
	store    *Store
	log      MessageLog
	key      ChannelKey
	state    ChannelStateStore
	groupKey isr.GroupKey
	prevHW   uint64
}

func newCheckpointBridge(base isr.CheckpointStore, store *Store, log MessageLog, key ChannelKey, state ChannelStateStore, groupKey isr.GroupKey) (*checkpointBridge, error) {
	checkpoint, err := base.Load()
	if err != nil && !errors.Is(err, isr.ErrEmptyState) {
		return nil, err
	}
	return &checkpointBridge{
		base:     base,
		store:    store,
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
	if b.store != nil {
		if coordinator := b.store.commitCoordinator(); coordinator != nil {
			if len(batch) == 0 || canBuildCheckpointState(b.state) {
				if err := coordinator.submit(commitRequest{
					groupKey: b.groupKey,
					build: func(writeBatch *pebble.Batch) error {
						if len(batch) > 0 {
							store := b.state.(batchCheckpointStateStore)
							return store.BuildCommitCommittedWithCheckpoint(writeBatch, checkpoint, batch)
						}
						return b.store.writeCheckpoint(writeBatch, checkpoint)
					},
					publish: func() error {
						b.store.recordDurableCommit()
						b.prevHW = checkpoint.HW
						return nil
					},
				}); err != nil {
					return err
				}
				return nil
			}
		}
	}
	if len(batch) > 0 {
		if store, ok := b.state.(atomicCheckpointStateStore); ok {
			if err := store.CommitCommittedWithCheckpoint(checkpoint, batch); err != nil {
				return err
			}
			b.prevHW = checkpoint.HW
			return nil
		}
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

func canBuildCheckpointState(state ChannelStateStore) bool {
	if state == nil {
		return false
	}
	_, ok := state.(batchCheckpointStateStore)
	return ok
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
		message, err := decodeMessageRecord(record)
		if err != nil {
			return nil, err
		}
		if message.ClientMsgNo == "" {
			continue
		}
		batch = append(batch, appliedMessage{
			key: IdempotencyKey{
				ChannelID:   b.key.ChannelID,
				ChannelType: b.key.ChannelType,
				FromUID:     message.FromUID,
				ClientMsgNo: message.ClientMsgNo,
			},
			entry: IdempotencyEntry{
				MessageID:  message.MessageID,
				MessageSeq: message.MessageSeq,
				Offset:     record.Offset,
			},
		})
	}
	return batch, nil
}

func (b *checkpointBridge) StoreApplyFetch(req isr.ApplyFetchStoreRequest) (uint64, error) {
	if b.store == nil {
		return 0, isr.ErrInvalidConfig
	}

	currentLEO, err := b.store.leo()
	if err != nil {
		return 0, err
	}

	committed, err := b.readCommittedBatchForApplyFetch(currentLEO, req)
	if err != nil {
		return 0, err
	}

	leo, err := b.store.applyFetchedRecords(req.Records, committed, req.Checkpoint)
	if err != nil {
		return 0, err
	}
	if req.Checkpoint != nil {
		b.prevHW = req.Checkpoint.HW
	}
	return leo, nil
}

func (b *checkpointBridge) readCommittedBatchForApplyFetch(base uint64, req isr.ApplyFetchStoreRequest) ([]appliedMessage, error) {
	if req.Checkpoint == nil || req.Checkpoint.HW <= b.prevHW {
		return nil, nil
	}

	nextHW := req.Checkpoint.HW
	batch := make([]appliedMessage, 0, int(nextHW-b.prevHW))

	existingUpper := minUint64(nextHW, base)
	if existingUpper > b.prevHW {
		records, err := b.log.Read(b.groupKey, b.prevHW, int(existingUpper-b.prevHW), math.MaxInt)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			msg, ok, err := appliedMessageFromLogRecord(b.key, record)
			if err != nil {
				return nil, err
			}
			if ok {
				batch = append(batch, msg)
			}
		}
	}

	newUpper := minUint64(nextHW, base+uint64(len(req.Records)))
	start := maxUint64(b.prevHW, base)
	for offset := start; offset < newUpper; offset++ {
		record := LogRecord{
			Offset:  offset,
			Payload: req.Records[offset-base].Payload,
		}
		msg, ok, err := appliedMessageFromLogRecord(b.key, record)
		if err != nil {
			return nil, err
		}
		if ok {
			batch = append(batch, msg)
		}
	}
	return batch, nil
}

func appliedMessageFromLogRecord(key ChannelKey, record LogRecord) (appliedMessage, bool, error) {
	message, err := decodeMessageRecord(record)
	if err != nil {
		return appliedMessage{}, false, err
	}
	if message.ClientMsgNo == "" {
		return appliedMessage{}, false, nil
	}
	return appliedMessage{
		key: IdempotencyKey{
			ChannelID:   key.ChannelID,
			ChannelType: key.ChannelType,
			FromUID:     message.FromUID,
			ClientMsgNo: message.ClientMsgNo,
		},
		entry: IdempotencyEntry{
			MessageID:  message.MessageID,
			MessageSeq: message.MessageSeq,
			Offset:     record.Offset,
		},
	}, true, nil
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
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
