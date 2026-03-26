package wkdb

import (
	"context"

	"github.com/cockroachdb/pebble"
)

func (db *DB) ExportSlotSnapshot(ctx context.Context, slotID uint64) (SlotSnapshot, error) {
	if err := validateSlot(slotID); err != nil {
		return SlotSnapshot{}, err
	}
	if err := db.checkContext(ctx); err != nil {
		return SlotSnapshot{}, err
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	view := db.db.NewSnapshot()
	defer view.Close()

	var entries []snapshotEntry
	for _, span := range slotAllDataSpans(slotID) {
		iter, err := view.NewIter(&pebble.IterOptions{
			LowerBound: span.Start,
			UpperBound: span.End,
		})
		if err != nil {
			return SlotSnapshot{}, err
		}

		for iter.First(); iter.Valid(); iter.Next() {
			if err := db.checkContext(ctx); err != nil {
				iter.Close()
				return SlotSnapshot{}, err
			}

			value, err := iter.ValueAndErr()
			if err != nil {
				iter.Close()
				return SlotSnapshot{}, err
			}

			entries = append(entries, snapshotEntry{
				Key:   append([]byte(nil), iter.Key()...),
				Value: append([]byte(nil), value...),
			})
		}
		if err := iter.Error(); err != nil {
			iter.Close()
			return SlotSnapshot{}, err
		}
		if err := iter.Close(); err != nil {
			return SlotSnapshot{}, err
		}
	}

	data, stats := encodeSlotSnapshotPayload(slotID, entries)
	return SlotSnapshot{
		SlotID: slotID,
		Data:   data,
		Stats:  stats,
	}, nil
}

func (db *DB) ImportSlotSnapshot(ctx context.Context, snap SlotSnapshot) error {
	if err := validateSlot(snap.SlotID); err != nil {
		return err
	}
	if err := db.checkContext(ctx); err != nil {
		return err
	}

	decoded, err := decodeSlotSnapshotPayload(snap.Data)
	if err != nil {
		return err
	}
	if decoded.SlotID != snap.SlotID {
		return ErrInvalidArgument
	}

	if err := db.DeleteSlotData(ctx, snap.SlotID); err != nil {
		return err
	}
	if err := db.checkContext(ctx); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	batch := db.db.NewBatch()
	defer batch.Close()

	for _, entry := range decoded.Entries {
		if err := batch.Set(entry.Key, entry.Value, nil); err != nil {
			return err
		}
	}

	if db.testHooks.beforeImportCommit != nil {
		if err := db.testHooks.beforeImportCommit(); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}
